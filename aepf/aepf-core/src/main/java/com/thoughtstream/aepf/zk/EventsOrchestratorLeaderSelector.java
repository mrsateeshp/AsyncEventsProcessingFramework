package com.thoughtstream.aepf.zk;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.QueuedEvent;
import com.thoughtstream.aepf.beans.Watermark;
import com.thoughtstream.aepf.handlers.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Revoker;
import org.apache.curator.framework.recipes.queue.DistributedIdQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;
import static com.thoughtstream.aepf.DefaultConstants.exec;
import static com.thoughtstream.aepf.zk.Constantants.APP_GLOBAL_LOCK_STR;
import static com.thoughtstream.aepf.zk.Constantants.LOCK_STR;
import static com.thoughtstream.aepf.zk.Constantants.QUEUES_STR;

/**
 * @author Sateesh Pinnamaneni
 * @since 07/01/2018
 */
class EventsOrchestratorLeaderSelector<T extends Event> extends LeaderSelectorListenerAdapter {
    private static final Logger log = LoggerFactory.getLogger(EventsOrchestrator.class);

    private final EventSourcerFactory<T> eventSourcerFactory;
    private final String watermarkPath;
    private final String eventSourceId;
    private volatile boolean hasLeadership = false;

    private final String watermarksRootPath;
    private final String outstandingTasksQueuePath;
    private final String outstandingTasksQueueLockPath;
    private final String completedTasksQueuePath;
    private final String completedTasksQueueLockPath;
    private final EventQueueSerializer<T> eventQueueSerializer;
    private final WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer;
    private final ShardKeyProvider<T> shardKeyProvider;
    private final int maxOutstandingEventsPerEventSource;
    private final InterProcessMutex applicationGlobalLock;
    private final CuratorFramework zkClient;

    EventsOrchestratorLeaderSelector(CuratorFramework zkClient, String zookeeperRoot, String applicationId,
                                     EventSourcerFactory<T> eventSourcerFactory, String watermarkPath, String eventSourceId,
                                     EventSerializerDeserializer<T> eventSerializerDeserializer,
                                     WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer,
                                     ShardKeyProvider<T> shardKeyProvider, int maxOutstandingEventsPerEventSource) {
        this.zkClient = zkClient;
        this.eventSourcerFactory = eventSourcerFactory;
        this.watermarkPath = watermarkPath;
        this.eventSourceId = eventSourceId;
        this.watermarkSerializerDeserializer = watermarkSerializerDeserializer;
        this.shardKeyProvider = shardKeyProvider;
        this.maxOutstandingEventsPerEventSource = maxOutstandingEventsPerEventSource;
        final String applicationPath = zookeeperRoot + "/" + applicationId;
        this.watermarksRootPath = applicationPath + "/" + Constantants.WATERMARK_STR;
        this.outstandingTasksQueuePath = applicationPath + "/" + QUEUES_STR + "/outstandingTasks";
        this.outstandingTasksQueueLockPath = outstandingTasksQueuePath + LOCK_STR;
        this.completedTasksQueuePath = applicationPath + "/" + QUEUES_STR + "/completedTasks";
        this.completedTasksQueueLockPath = completedTasksQueuePath + LOCK_STR;
        this.eventQueueSerializer = new EventQueueSerializer<>(eventSerializerDeserializer);

        this.applicationGlobalLock = new InterProcessMutex(zkClient, applicationPath + "/" + APP_GLOBAL_LOCK_STR);
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {

        InterProcessMutex watermarkGlobalLock = null;
        DistributedIdQueue<QueuedEvent<T>> completedTasksQueue = null;
        DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueue = null;

        try {
            hasLeadership = true;

            log.info("Got leadership for EventSourcer: " + eventSourcerFactory.getSourceId());
            String watermarkGlobalLockPath = watermarkPath + LOCK_STR;
            Revoker.attemptRevoke(zkClient, watermarkGlobalLockPath);
            watermarkGlobalLock = new InterProcessMutex(zkClient, watermarkGlobalLockPath);
            watermarkGlobalLock.acquire(5, TimeUnit.MINUTES);
            watermarkGlobalLock.makeRevocable(interProcessMutex -> hasLeadership = false);

            log.info("Got watermark global lock for EventSourcer: " + eventSourceId);

            outstandingTasksQueue = QueueBuilder.builder(zkClient, null, eventQueueSerializer, outstandingTasksQueuePath)
                    .lockPath(outstandingTasksQueueLockPath).buildIdQueue();
            outstandingTasksQueue.start();

            final DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueueFinal = outstandingTasksQueue;
            completedTasksQueue = QueueBuilder.builder(zkClient, new CompletedEventsQueuedConsumer(outstandingTasksQueueFinal), eventQueueSerializer, completedTasksQueuePath)
                    .lockPath(completedTasksQueueLockPath).buildIdQueue();
            completedTasksQueue.start();

            byte[] watermarkBytes = zkClient.getData().forPath(watermarkPath);
            Watermark<T> watermark = watermarkSerializerDeserializer.deserialize(watermarkBytes);

            EventSourcer<T> eventSourcer = null;
            try {
                eventSourcer = eventSourcerFactory.create(watermark.getLastEvent());
                while (hasLeadership) {
                    while (watermarkSerializerDeserializer.deserialize(zkClient.getData().forPath(watermarkPath))
                            .getOutstandingEvents().values().stream().mapToInt(LinkedList::size).sum() >= maxOutstandingEventsPerEventSource) {
                        log.warn("Reached max limit[{}] of outstanding events in the queue! hence waiting for 5min before trying again!", maxOutstandingEventsPerEventSource);
                        try {
                            Thread.sleep(60 * 1000);
                        } catch (Exception e) {
                            //ignore
                        }
                    }

                    Optional<T> nextEventOpt = Optional.empty();

                    while (hasLeadership && !nextEventOpt.isPresent()) {
                        nextEventOpt = eventSourcer.nextEvent(5 * 60 * 1000);
                    }

                    if (!hasLeadership || !nextEventOpt.isPresent()) {
                        throw new CancelLeadershipException();
                    }

                    T eventToProcess = nextEventOpt.get();
                    log.info("processing the event: {}", eventToProcess);


                    try {
                        applicationGlobalLock.acquire(5, TimeUnit.MINUTES);
                        Stat watermarkStat = new Stat();
                        final byte[] currentWatermarkBytes = zkClient.getData().storingStatIn(watermarkStat).forPath(watermarkPath);
                        final int versionOfDataBeforeUpdate = watermarkStat.getVersion();
                        log.info("[ORCHESTRATOR] for [{}] loaded the following watermark: {}", watermarkPath, new String(currentWatermarkBytes, DEFAULT_CHAR_SET));
                        final Watermark<T> currentWatermark = watermarkSerializerDeserializer.deserialize(currentWatermarkBytes);
                        final String shardKey = shardKeyProvider.getShardKey(eventToProcess);
                        boolean isEligibleForWorkerQueue = !currentWatermark.getOutstandingEvents().containsKey(shardKey);
                        final Watermark<T> updatedWatermark = new Watermark<>(eventToProcess, currentWatermark.getOutstandingEvents());
                        updatedWatermark.getOutstandingEvents().putIfAbsent(shardKey, new LinkedList<>());
                        updatedWatermark.getOutstandingEvents().get(shardKey).add(eventToProcess);

                        byte[] updatedWatermarkBytes = watermarkSerializerDeserializer.serialize(updatedWatermark);

                        if (hasLeadership) { // there is a chance leadership was lost while
                            if (isEligibleForWorkerQueue) {
                                //place it on the worker queue or else, just
                                outstandingTasksQueueFinal.put(new QueuedEvent<>(eventSourceId, eventToProcess), "FIXME");
                                log.info("[ORCHESTRATOR] placed the following on outstandingTasksQueue: {}", eventToProcess);
                            }

                            //using optimistic locking - withVersion will compare the current version of the data before updating it.
                            zkClient.setData().withVersion(versionOfDataBeforeUpdate).forPath(watermarkPath, updatedWatermarkBytes);
                            log.info("[ORCHESTRATOR] updated watermark for [{}]: {}", watermarkPath, new String(updatedWatermarkBytes, DEFAULT_CHAR_SET));
                        }
                    } finally {
                        exec(applicationGlobalLock::release);
                    }
                }
            } finally {
                if (eventSourcer != null) {
                    exec(eventSourcer::close);
                }
            }
        } catch (Exception e) {
            log.error("[ORCHESTRATOR] error processing.", e);
            throw new CancelLeadershipException(e);
        } finally {
            hasLeadership = false;
            log.info("Relinquishing leadership for EventSourcer!" + eventSourceId);
            if (watermarkGlobalLock != null) {
                exec(watermarkGlobalLock::release);
            }

            if (outstandingTasksQueue != null) {
                exec(outstandingTasksQueue::close);
            }
            if (completedTasksQueue != null) {
                exec(completedTasksQueue::close);
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState) {
        if (newState != ConnectionState.CONNECTED) {
            hasLeadership = false;
        }
        log.info("Connection state changed to: " + newState.name());
        super.stateChanged(curatorFramework, newState);
    }

    private class CompletedEventsQueuedConsumer implements QueueConsumer<QueuedEvent<T>> {
        private final DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueueFinal;

        private CompletedEventsQueuedConsumer(DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueueFinal) {
            this.outstandingTasksQueueFinal = outstandingTasksQueueFinal;
        }

        @Override
        public void consumeMessage(QueuedEvent<T> message) throws Exception {
            log.info("[CONSUMER] received the following event: {}", message);

            try {
                applicationGlobalLock.acquire(5, TimeUnit.MINUTES);
                final String localEventSourceId = message.getEventSourceId();
                final String localWatermarkPath = watermarksRootPath + "/" + localEventSourceId;
                final T event = message.getEvent();
                final String shardKey = shardKeyProvider.getShardKey(event);
                final Stat watermarkStat = new Stat();
                final byte[] currentWatermarkBytes = zkClient.getData().storingStatIn(watermarkStat).forPath(localWatermarkPath);
                final int versionOfDataBeforeUpdate = watermarkStat.getVersion();
                log.info("[CONSUMER] watermark loaded from [{}]: {}", localWatermarkPath, new String(currentWatermarkBytes, DEFAULT_CHAR_SET));
                final Watermark<T> currentWatermark = watermarkSerializerDeserializer.deserialize(currentWatermarkBytes);
                final LinkedList<T> events = currentWatermark.getOutstandingEvents().get(shardKey);
                if (events != null && !events.isEmpty() && events.get(0).equals(event)) {
                    log.info("[CONSUMER] event found in the watermark: {}", message);
                    events.removeFirst();
                    T eventToProcess = null;
                    if (!events.isEmpty()) {
                        eventToProcess = events.getFirst();

                    } else {
                        currentWatermark.getOutstandingEvents().remove(shardKey);
                        log.info("[CONSUMER] following shardKey is removed from the watermark: {}", shardKey);
                    }

                    if (hasLeadership) {
                        if (eventToProcess != null) {
                            outstandingTasksQueueFinal.put(new QueuedEvent<>(localEventSourceId, eventToProcess), "FIXME");
                            log.info("[CONSUMER] placed the following event on outstanding queue: {}", eventToProcess);
                        }

                        byte[] updatedWatermarkBytes = watermarkSerializerDeserializer.serialize(currentWatermark);
                        //using optimistic locking - withVersion will compare the current version of the data before updating it.
                        zkClient.setData().withVersion(versionOfDataBeforeUpdate).forPath(localWatermarkPath, updatedWatermarkBytes);
                        log.info("[CONSUMER] watermark updated for {}: {}", localWatermarkPath, new String(updatedWatermarkBytes, DEFAULT_CHAR_SET));
                    } else {
                        throw new RuntimeException("Leadership lost!");
                    }
                } else {
                    log.error("[CONSUMER] event not found as first element in the watermark, that means it is a duplicate event: {}", message);
                }
            } finally {
                exec(applicationGlobalLock::release);
            }
        }

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (connectionState != ConnectionState.CONNECTED) {
                hasLeadership = false;
            }
        }
    }
}
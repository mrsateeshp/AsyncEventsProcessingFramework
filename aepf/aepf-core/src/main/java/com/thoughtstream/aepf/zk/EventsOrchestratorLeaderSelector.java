package com.thoughtstream.aepf.zk;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.QueuedEvent;
import com.thoughtstream.aepf.beans.Watermark;
import com.thoughtstream.aepf.handlers.*;
import io.prometheus.client.Gauge;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.queue.DistributedIdQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;
import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 07/01/2018
 */
class EventsOrchestratorLeaderSelector<T extends Event> extends LeaderSelectorListenerAdapter {
    private static final Logger log = LoggerFactory.getLogger(EventsOrchestrator.class);

    private static final Gauge outstandingEventShardsGauge = Gauge.build().namespace("leader").name("outstanding_event_shards")
            .labelNames("srcId").help("No of event shards waiting to be processed!").register();

    private static final Gauge outstandingEventsGauge = Gauge.build().namespace("leader").name("outstanding_events")
            .labelNames("srcId").help("No of events waiting to be processed!").register();

    private final EventSourcerFactory<T> eventSourcerFactory;
    private final String watermarkPath;
    private final String eventSourceId;
    private volatile boolean hasLeadership = false;

    private final Object hasLeadershipLock = new Object();

    private final EventQueueSerializer<T> eventQueueSerializer;
    private final WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer;
    private final ShardKeyProvider<T> shardKeyProvider;
    private final int maxOutstandingEventsPerEventSource;

    private final CuratorFramework zkClient;
    private final InterProcessMutex watermarkGlobalLock;
    private final ZkPathsProvider zkPathsProvider;

    EventsOrchestratorLeaderSelector(CuratorFramework zkClient, ZkPathsProvider zkPathsProvider,
                                     EventSourcerFactory<T> eventSourcerFactory, String eventSourceId,
                                     EventSerializerDeserializer<T> eventSerializerDeserializer,
                                     WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer,
                                     ShardKeyProvider<T> shardKeyProvider, int maxOutstandingEventsPerEventSource) {
        this.zkClient = zkClient;
        this.zkPathsProvider = zkPathsProvider;
        this.eventSourcerFactory = eventSourcerFactory;
        this.watermarkPath = zkPathsProvider.getWatermarkPath(eventSourceId);
        this.eventSourceId = eventSourceId;
        this.watermarkSerializerDeserializer = watermarkSerializerDeserializer;
        this.shardKeyProvider = shardKeyProvider;
        this.maxOutstandingEventsPerEventSource = maxOutstandingEventsPerEventSource;
        this.eventQueueSerializer = new EventQueueSerializer<>(eventSerializerDeserializer);
        this.watermarkGlobalLock = new InterProcessMutex(zkClient, zkPathsProvider.getWatermarkLockPath(eventSourceId));
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {

        DistributedIdQueue<QueuedEvent<T>> completedTasksQueue = null;
        DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueue = null;

        try {
            synchronized (hasLeadershipLock) {
                if (!Thread.interrupted()) {
                    hasLeadership = true;
                } else {
                    throw new CancelLeadershipException();
                }
            }

            log.info("Got leadership for EventSourcer: " + eventSourcerFactory.getSourceId());

            log.info("Got watermark global lock for EventSourcer: " + eventSourceId);

            outstandingTasksQueue = QueueBuilder.builder(zkClient, null, eventQueueSerializer, zkPathsProvider.getOutstandingTasksQueuePath())
                    .lockPath(zkPathsProvider.getOutstandingTasksQueueLockPath()).buildIdQueue();
            outstandingTasksQueue.start();

            CompletedEventsQueuedConsumer<T> consumer = new CompletedEventsQueuedConsumer<>(zkClient, outstandingTasksQueue, watermarkSerializerDeserializer, shardKeyProvider, zkPathsProvider);
            completedTasksQueue = QueueBuilder.builder(zkClient, consumer, eventQueueSerializer, zkPathsProvider.getCompletedTasksQueuePath())
                    .lockPath(zkPathsProvider.getCompletedTasksQueueLockPath()).buildIdQueue();
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
                        watermarkGlobalLock.acquire(5, TimeUnit.MINUTES);
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
                                outstandingTasksQueue.put(new QueuedEvent<>(eventSourceId, eventToProcess), "FIXME");
                                log.info("[ORCHESTRATOR] placed the following on outstandingTasksQueue: {}", eventToProcess);
                            }

                            //using optimistic locking - withVersion will compare the current version of the data before updating it.
                            zkClient.setData().withVersion(versionOfDataBeforeUpdate).forPath(watermarkPath, updatedWatermarkBytes);
                            log.info("[ORCHESTRATOR] updated watermark for [{}]: {}", watermarkPath, new String(updatedWatermarkBytes, DEFAULT_CHAR_SET));
                        }

                        outstandingEventShardsGauge.set(updatedWatermark.getOutstandingEvents().keySet().size());
                        outstandingEventsGauge.set(updatedWatermark.getOutstandingEvents().values().stream().mapToInt(LinkedList::size).sum());
                    } finally {
                        exec(watermarkGlobalLock::release);
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

}
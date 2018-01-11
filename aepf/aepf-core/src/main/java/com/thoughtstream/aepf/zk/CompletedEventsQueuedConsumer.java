package com.thoughtstream.aepf.zk;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.QueuedEvent;
import com.thoughtstream.aepf.beans.Watermark;
import com.thoughtstream.aepf.handlers.ShardKeyProvider;
import com.thoughtstream.aepf.handlers.WatermarkSerializerDeserializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.queue.DistributedIdQueue;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;
import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 11/01/2018
 */
class CompletedEventsQueuedConsumer<T extends Event> implements QueueConsumer<QueuedEvent<T>> {
    private static final Logger log = LoggerFactory.getLogger(CompletedEventsQueuedConsumer.class);
    private final DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueueFinal;

    private final CuratorFramework zkClient;
    private final WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer;
    private final ShardKeyProvider<T> shardKeyProvider;
    private final ZkPathsProvider zkPathsProvider;

    CompletedEventsQueuedConsumer(CuratorFramework zkClient, DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueueFinal,
                                  WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer,
                                  ShardKeyProvider<T> shardKeyProvider, ZkPathsProvider zkPathsProvider) {
        this.zkClient = zkClient;
        this.outstandingTasksQueueFinal = outstandingTasksQueueFinal;
        this.watermarkSerializerDeserializer = watermarkSerializerDeserializer;
        this.shardKeyProvider = shardKeyProvider;
        this.zkPathsProvider = zkPathsProvider;
    }

    @Override
    public void consumeMessage(QueuedEvent<T> message) throws Exception {
        log.info("[CONSUMER] received the following event: {}", message);

        final String localEventSourceId = message.getEventSourceId();
        final String localWatermarkPath = zkPathsProvider.getWatermarkPath(localEventSourceId);
        final String localWatermarkLockPath = zkPathsProvider.getWatermarkLockPath(localEventSourceId);

        final InterProcessMutex consumerWatermarkGlobalLock = new InterProcessMutex(zkClient, localWatermarkLockPath);

        try {
            consumerWatermarkGlobalLock.acquire(5, TimeUnit.MINUTES);

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

                if (eventToProcess != null) {
                    outstandingTasksQueueFinal.put(new QueuedEvent<>(localEventSourceId, eventToProcess), "FIXME");
                    log.info("[CONSUMER] placed the following event on outstanding queue: {}", eventToProcess);
                }

                byte[] updatedWatermarkBytes = watermarkSerializerDeserializer.serialize(currentWatermark);
                //using optimistic locking - withVersion will compare the current version of the data before updating it.
                zkClient.setData().withVersion(versionOfDataBeforeUpdate).forPath(localWatermarkPath, updatedWatermarkBytes);
                log.info("[CONSUMER] watermark updated for {}: {}", localWatermarkPath, new String(updatedWatermarkBytes, DEFAULT_CHAR_SET));

            } else {
                log.error("[CONSUMER] event not found as first element in the watermark, that means it is a duplicate event: {}", message);
            }
        } finally {
            exec(consumerWatermarkGlobalLock::release);
        }
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        //ignored
    }
}

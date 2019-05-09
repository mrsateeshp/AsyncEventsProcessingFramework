package com.thoughtstream.aepf.zk;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.QueuedEvent;
import com.thoughtstream.aepf.handlers.EventHandler;
import com.thoughtstream.aepf.handlers.EventSerializerDeserializer;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedIdQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class EventWorker<T extends Event> {
    private static final Logger log = LoggerFactory.getLogger(EventWorker.class);

    private static Counter processedReqCounter = Counter.build().namespace("worker").name("processed_requests")
            .labelNames("srcId").help("counts no of requests processed.").register();

    private static Counter failedReqCounter = Counter.build().namespace("worker").name("failed_requests")
            .labelNames("srcId").help("counts no of requests failed.").register();

    private static Histogram processingTime = Histogram.build().namespace("worker").name("request_duration_sec")
            .labelNames("srcId").help("Request processing time histogram.").register();


    private final EventQueueSerializer<T> eventQueueSerializer;
    private final EventHandler<T> eventHandler;

    private DistributedIdQueue<QueuedEvent<T>> completedTasksQueue = null;
    private DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueue = null;

    private final CuratorFramework zkClient;
    private final ZkPathsProvider zkPathsProvider;

    public EventWorker(CuratorFramework zkClient, ZkPathsProvider zkPathsProvider,
                       EventSerializerDeserializer<T> eventSerializerDeserializer, EventHandler<T> eventHandler) {
        this.zkClient = zkClient;
        this.zkPathsProvider = zkPathsProvider;
        this.eventHandler = eventHandler;
        this.eventQueueSerializer = new EventQueueSerializer<>(eventSerializerDeserializer);
    }

    public boolean isHealthy() {
        //FIXME
        return true;
    }

    public void stop() throws Exception {
        if (outstandingTasksQueue != null) {
            exec(outstandingTasksQueue::close);
        }

        if (completedTasksQueue != null) {
            exec(completedTasksQueue::close);
        }
    }

    public void start() throws Exception {
        if (outstandingTasksQueue != null || completedTasksQueue != null) {
            //FIXME:
            throw new RuntimeException("Running already!");
        }

        String applicationPath = zkPathsProvider.getApplicationPath();
        Stat applicationExists = zkClient.checkExists().forPath(applicationPath);

        if (applicationExists == null) {
            throw new RuntimeException("Supplied Zookeeper Node Path for application does not exist: " + applicationPath);
        }

        try {
            completedTasksQueue = QueueBuilder.builder(zkClient, null, eventQueueSerializer, zkPathsProvider.getCompletedTasksQueuePath())
                    .lockPath(zkPathsProvider.getCompletedTasksQueueLockPath())
                    .buildIdQueue();
            completedTasksQueue.start();

            DistributedIdQueue<QueuedEvent<T>> completedTasksQueueFinal = this.completedTasksQueue;

            outstandingTasksQueue = QueueBuilder.builder(zkClient, new QueueConsumer<QueuedEvent<T>>() {
                @Override
                public void consumeMessage(QueuedEvent<T> message) throws Exception {
                    String eventSourceId = message.getEventSourceId();
                    try {
                        log.info("[RECEIVED]Processing: {}", message);

                        long startTime = System.currentTimeMillis();
                        eventHandler.process(message.getEvent());
                        long endTime = System.currentTimeMillis();

                        completedTasksQueueFinal.put(message, "FIXME");
                        processedReqCounter.labels(eventSourceId).inc();
                        processingTime.labels(eventSourceId)
                                .observe(TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.MILLISECONDS));
                        log.info("[PROCESSED]Finished processing: {}", message);
                    } catch (Exception e) {
                        failedReqCounter.labels(eventSourceId).inc();
                        throw e;
                    }
                }

                @Override
                public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                    log.info("Connection state changed to: " + connectionState.name());
                }
            }, eventQueueSerializer, zkPathsProvider.getOutstandingTasksQueuePath())
                    .lockPath(zkPathsProvider.getOutstandingTasksQueueLockPath())
                    .buildIdQueue();
            outstandingTasksQueue.start();
        } catch (Exception e) {
            log.error("Exception: ", e);
            throw e;
        }
    }
}

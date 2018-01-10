package com.thoughtstream.aepf.zk;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.QueuedEvent;
import com.thoughtstream.aepf.handlers.EventHandler;
import com.thoughtstream.aepf.handlers.EventSerializerDeserializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedIdQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class EventWorker<T extends Event> {
    private static final Logger log = LoggerFactory.getLogger(EventWorker.class);

    private final static String WATERMARK_STR = "watermarks";
    private final static String QUEUES_STR = "queues";
    private final static String LOCK_STR = "-lock";

    private final String applicationPath;
    private final String outstandingTasksQueuePath;
    private final String outstandingTasksQueueLockPath;
    private final String completedTasksQueuePath;
    private final String completedTasksQueueLockPath;
    private final EventQueueSerializer<T> eventQueueSerializer;
    private final EventSerializerDeserializer<T> eventSerializerDeserializer;
    private final EventHandler<T> eventHandler;

    private DistributedIdQueue<QueuedEvent<T>> completedTasksQueue = null;
    private DistributedIdQueue<QueuedEvent<T>> outstandingTasksQueue = null;

    private final CuratorFramework zkClient;

    public EventWorker(CuratorFramework zkClient, String watermarksRoot, String applicationId,
                       EventSerializerDeserializer<T> eventSerializerDeserializer, EventHandler<T> eventHandler) {
        this.zkClient = zkClient;
        this.eventSerializerDeserializer = eventSerializerDeserializer;
        this.eventHandler = eventHandler;
        this.applicationPath = watermarksRoot + "/" + applicationId;
        this.outstandingTasksQueuePath = applicationPath + "/" + QUEUES_STR + "/outstandingTasks";
        this.outstandingTasksQueueLockPath = outstandingTasksQueuePath + LOCK_STR;
        this.completedTasksQueuePath = applicationPath + "/" + QUEUES_STR + "/completedTasks";
        this.completedTasksQueueLockPath = completedTasksQueuePath + LOCK_STR;
        this.eventQueueSerializer = new EventQueueSerializer<>(this.eventSerializerDeserializer);
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

        Stat applicationExists = zkClient.checkExists().forPath(applicationPath);

        if (applicationExists == null) {
            throw new RuntimeException("Supplied Zookeeper Node Path for application does not exist: " + applicationPath);
        }

        try {
            completedTasksQueue = QueueBuilder.builder(zkClient, null, eventQueueSerializer, completedTasksQueuePath)
                    .lockPath(completedTasksQueueLockPath)
                    .buildIdQueue();
            completedTasksQueue.start();

            DistributedIdQueue<QueuedEvent<T>> completedTasksQueueFinal = this.completedTasksQueue;

            outstandingTasksQueue = QueueBuilder.builder(zkClient, new QueueConsumer<QueuedEvent<T>>() {
                @Override
                public void consumeMessage(QueuedEvent<T> message) throws Exception {
                    log.info("Processing: {}", message);
                    eventHandler.process(message.getEvent());
                    completedTasksQueueFinal.put(message, "FIXME");
                    log.info("Finished processing: {}", message);
                }

                @Override
                public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                    log.info("Connection state changed to: " + connectionState.name());
                }
            }, eventQueueSerializer, outstandingTasksQueuePath)
                    .lockPath(outstandingTasksQueueLockPath)
                    .buildIdQueue();
            outstandingTasksQueue.start();
        } catch (Exception e) {
            log.error("Exception: ", e);
            throw e;
        }
    }
}
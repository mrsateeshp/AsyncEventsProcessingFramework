package com.thoughtstream.aepf;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.handlers.EventHandler;
import com.thoughtstream.aepf.handlers.EventSerializerDeserializer;
import com.thoughtstream.aepf.handlers.EventSourcerFactory;
import com.thoughtstream.aepf.handlers.ShardKeyProvider;
import com.thoughtstream.aepf.zk.EventWorker;
import com.thoughtstream.aepf.zk.EventsOrchestrator;
import com.thoughtstream.aepf.zk.ZkPathsProvider;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class AsyncEventsProcessor<T extends Event> {
    private static final Logger log = LoggerFactory.getLogger(AsyncEventsProcessor.class);

    private final String zookeeperConnectionStr;
    private final String zookeeperRoot;
    private final String applicationId;
    private final Collection<EventSourcerFactory<T>> eventSourcerFactories;
    private final EventSerializerDeserializer<T> eventSerializerDeserializer;
    private final ShardKeyProvider<T> shardKeyProvider;
    private final EventHandler<T> eventHandler;
    private final int maxOutstandingEventsPerEventSource;
    private final boolean runAsDaemon;
    private volatile boolean isRunning = false;
    private volatile AsyncEventsProcessorRunnable currentWorker = null;
    private final ThreadGroup threadGroup = new ThreadGroup("AsyncEventsProcessorThread");

    public AsyncEventsProcessor(String zookeeperConnectionStr, String zookeeperRoot, String applicationId,
                                Collection<EventSourcerFactory<T>> eventSourcerFactories,
                                EventSerializerDeserializer<T> eventSerializerDeserializer,
                                ShardKeyProvider<T> shardKeyProvider, EventHandler<T> eventHandler,
                                int maxOutstandingEventsPerEventSource, boolean runAsDaemon) {
        this.zookeeperConnectionStr = zookeeperConnectionStr;
        this.zookeeperRoot = zookeeperRoot;
        this.applicationId = applicationId;
        this.eventSourcerFactories = eventSourcerFactories;
        this.eventSerializerDeserializer = eventSerializerDeserializer;
        this.shardKeyProvider = shardKeyProvider;
        this.eventHandler = eventHandler;
        this.maxOutstandingEventsPerEventSource = maxOutstandingEventsPerEventSource;
        this.runAsDaemon = runAsDaemon;
    }

    public synchronized void stop() {
        if (currentWorker != null) {
            exec(currentWorker::stop);
        }
        isRunning = false;
    }

    public synchronized void start() {
        if (isRunning) {
            throw new RuntimeException("already running!");
        }

        try {
            Thread thread = new Thread(threadGroup, new AsyncEventsProcessorRunnable());
            thread.setDaemon(runAsDaemon);
            thread.start();
            isRunning = true;
        } catch (Exception e) {
            log.error("Got exception while starting, hence stopping: ", e);
            stop();
            throw new RuntimeException(e);
        }
    }

    private class AsyncEventsProcessorRunnable implements Runnable {
        private volatile boolean runnableStop = false;

        @Override
        public synchronized void run() {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework zkClient = null;
            EventWorker<T> eventWorker = null;
            EventsOrchestrator<T> eventsOrchestrator = null;

            try {
                zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionStr, retryPolicy);
                zkClient.start();

                ZkPathsProvider zkPathsProvider = new ZkPathsProvider(zookeeperRoot, applicationId);
                eventWorker = new EventWorker<>(zkClient, zkPathsProvider, eventSerializerDeserializer, eventHandler);
                eventsOrchestrator = new EventsOrchestrator<>(zkClient, zkPathsProvider, eventSourcerFactories,
                        eventSerializerDeserializer, shardKeyProvider, maxOutstandingEventsPerEventSource);

                eventWorker.start();
                eventsOrchestrator.start();

                while (!runnableStop && eventWorker.isHealthy() && eventsOrchestrator.isHealthy()) {
                    exec(() -> Thread.sleep(5000));
                }
            } catch (Exception e) {
                log.error("Exception: ", e);
            } finally {
                if (eventWorker != null) {
                    exec(eventWorker::stop);
                }
                if (eventsOrchestrator != null) {
                    exec(eventsOrchestrator::stop);
                }
                if (zkClient != null) {
                    exec(zkClient::close);
                }
            }
        }

        synchronized void stop() {
            runnableStop = true;
        }
    }

}

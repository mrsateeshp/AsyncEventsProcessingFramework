package com.thoughtstream.aepf.zk;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.Watermark;
import com.thoughtstream.aepf.handlers.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;
import static com.thoughtstream.aepf.zk.Constantants.WATERMARK_STR;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public class EventsOrchestrator<T extends Event> {
    private static final Logger log = LoggerFactory.getLogger(EventsOrchestrator.class);

    private final String applicationPath;
    private final String watermarksRootPath;
    private final EventSerializerDeserializer<T> eventSerializerDeserializer;
    private final WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer;
    private final Collection<EventSourcerFactory<T>> eventSourcerFactories;
    private final ShardKeyProvider<T> shardKeyProvider;
    private final int maxOutstandingEventsPerEventSource;
    private final String applicationId;
    private final String zookeeperRoot;


    private final CuratorFramework zkClient;


    public EventsOrchestrator(CuratorFramework zkClient, String zookeeperRoot, String applicationId,
                              Collection<EventSourcerFactory<T>> eventSourcerFactories, EventSerializerDeserializer<T> eventSerializerDeserializer,
                              ShardKeyProvider<T> shardKeyProvider, int maxOutstandingEventsPerEventSource) {
        this.zkClient = zkClient;
        this.zookeeperRoot = zookeeperRoot;
        this.applicationId = applicationId;
        this.shardKeyProvider = shardKeyProvider;
        this.eventSerializerDeserializer = eventSerializerDeserializer;
        this.maxOutstandingEventsPerEventSource = maxOutstandingEventsPerEventSource;
        this.watermarkSerializerDeserializer = new DefaultWatermarkSerializerDeserializer<>(eventSerializerDeserializer);
        this.eventSourcerFactories = eventSourcerFactories;
        this.applicationPath = zookeeperRoot + "/" + applicationId;
        this.watermarksRootPath = applicationPath + "/" + WATERMARK_STR;
        //FIXME: validate eventSourcerFactories, such as duplicates, etc..
    }

    public boolean isHealthy() {
        //FIXME
        return true;
    }

    public void stop() throws Exception {
        //FIXME
    }

    public void start() throws Exception {

        Stat applicationExists = zkClient.checkExists().forPath(applicationPath);

        if (applicationExists == null) {
            throw new RuntimeException("Supplied Zookeeper Node Path for application does not exist: " + applicationPath);
        }

        if (zkClient.checkExists().forPath(watermarksRootPath) == null) {
            log.info("Watermark root not found, creating: " + watermarksRootPath); //FIXME: make this behavior configurable, client may not want auto watermark created
            zkClient.create().forPath(watermarksRootPath, "".getBytes(DEFAULT_CHAR_SET));
        }

        for (EventSourcerFactory<T> eventSourcerFactory : eventSourcerFactories) { //FIXME: delay between event sourcers
            final String eventSourceId = eventSourcerFactory.getSourceId();
            final String watermarkPath = watermarksRootPath + "/" + eventSourcerFactory.getSourceId();

            if (zkClient.checkExists().forPath(watermarkPath) == null) {
                log.info("Watermark not found for eventSource[{}], creating: {}", eventSourcerFactory.getSourceId(), watermarkPath);
                zkClient.create().forPath(watermarkPath, watermarkSerializerDeserializer.serialize(new Watermark<>()));
            }

            LeaderSelectorListener listener = new EventsOrchestratorLeaderSelector<>(zkClient, zookeeperRoot, applicationId,
                    eventSourcerFactory, watermarkPath, eventSourceId, eventSerializerDeserializer, watermarkSerializerDeserializer,
                    shardKeyProvider, maxOutstandingEventsPerEventSource);

            LeaderSelector selector = new LeaderSelector(zkClient, watermarkPath, listener);
            selector.autoRequeue();
            selector.start();
        }
    }
}

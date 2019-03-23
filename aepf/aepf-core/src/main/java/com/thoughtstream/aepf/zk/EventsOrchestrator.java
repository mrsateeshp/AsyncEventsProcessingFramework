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
import java.util.LinkedList;
import java.util.List;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;
import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public class EventsOrchestrator<T extends Event> {
    private static final Logger log = LoggerFactory.getLogger(EventsOrchestrator.class);

    private final List<LeaderSelector> leaderSelectorList = new LinkedList<>();

    public EventsOrchestrator(CuratorFramework zkClient, ZkPathsProvider zkPathsProvider,
                              Collection<EventSourcerFactory<T>> eventSourcerFactories, EventSerializerDeserializer<T> eventSerializerDeserializer,
                              ShardKeyProvider<T> shardKeyProvider, int maxOutstandingEventsPerEventSource) {

        WatermarkSerializerDeserializer<T> watermarkSerializerDeserializer = new DefaultWatermarkSerializerDeserializer<>(eventSerializerDeserializer);
        String watermarksRootPath = zkPathsProvider.getWatermarksPath();


        try {
            String applicationPath = zkPathsProvider.getApplicationPath();
            Stat applicationExists = zkClient.checkExists().forPath(applicationPath);

            if (applicationExists == null) {
                //FIXME:
//                throw new RuntimeException("Supplied Zookeeper Node Path for application does not exist: " + applicationPath);
                try {
                    zkClient.create().forPath("/rs");
                    zkClient.create().forPath(applicationPath);
                } catch (Exception e) {
                    if(zkClient.checkExists().forPath(applicationPath) == null){
                        throw new RuntimeException(e);
                    }
                }
            }

            if (zkClient.checkExists().forPath(watermarksRootPath) == null) {
                log.info("Watermark root not found, creating: " + watermarksRootPath); //FIXME: make this behavior configurable, client may not want auto watermark created
                zkClient.create().forPath(watermarksRootPath, "".getBytes(DEFAULT_CHAR_SET));
            }

            for (EventSourcerFactory<T> eventSourcerFactory : eventSourcerFactories) { //FIXME: delay between event sourcers
                final String eventSourceId = eventSourcerFactory.getSourceId();
                final String watermarkPath = zkPathsProvider.getWatermarkPath(eventSourcerFactory.getSourceId());

                if (zkClient.checkExists().forPath(watermarkPath) == null) {
                    log.info("Watermark not found for eventSource[{}], creating: {}", eventSourcerFactory.getSourceId(), watermarkPath);
                    zkClient.create().forPath(watermarkPath, watermarkSerializerDeserializer.serialize(new Watermark<>()));
                }

                LeaderSelectorListener listener = new EventsOrchestratorLeaderSelector<>(zkClient, zkPathsProvider,
                        eventSourcerFactory, eventSourceId, eventSerializerDeserializer, watermarkSerializerDeserializer,
                        shardKeyProvider, maxOutstandingEventsPerEventSource);

                LeaderSelector selector = new LeaderSelector(zkClient, watermarkPath, listener);
                selector.autoRequeue();
                leaderSelectorList.add(selector);
            }
        } catch (Exception e) {
            throw new RuntimeException(e); //FIXME: enxception types
        }
    }

    public boolean isHealthy() {
        //FIXME
        return true;
    }

    public void stop() throws Exception {
        for (LeaderSelector leaderSelector : leaderSelectorList) {
            exec(leaderSelector::close);
        }
    }

    public void start() throws Exception {
        leaderSelectorList.forEach(LeaderSelector::start);
    }
}

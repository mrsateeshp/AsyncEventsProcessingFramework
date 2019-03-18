package com.thoughtstream.aepf;

import com.thoughtstream.aepf.handlers.EventSourcerFactory;
import kamon.Kamon;
import kamon.prometheus.PrometheusReporter;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class AsyncEventsProcessorTest {
    public static void main(String[] args) throws Exception {
        PrometheusReporter prometheusReporter = new PrometheusReporter();
        Kamon.addReporter(prometheusReporter);

        List<EventSourcerFactory<IndexBasedEvent>> eventSourcerFactories = new LinkedList<>();
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer1"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer2"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer3"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer4"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer5"));

        IndexBasedEventSerializerDeserializer indexBasedEventSerializerDeserializer = new IndexBasedEventSerializerDeserializer();
        IndexBasedShardKeyProvider shardKeyProvider = new IndexBasedShardKeyProvider();
        IndexBasedEventHandler indexBasedEventHandler = new IndexBasedEventHandler();
        String zookeeperConnectionStr = System.getProperty("zkConStr", "host.docker.internal") ;
        AsyncEventsProcessor<IndexBasedEvent> processor = new AsyncEventsProcessor<>(zookeeperConnectionStr, "/rs", "aepf-test", eventSourcerFactories,
                indexBasedEventSerializerDeserializer, shardKeyProvider, indexBasedEventHandler, 50, false);
        processor.start();

        Thread.sleep(600000000); //FIXME:
    }
}
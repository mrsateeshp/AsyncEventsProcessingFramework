package com.thoughtstream.aepf.testimpl;

import com.thoughtstream.aepf.AsyncEventsProcessor;
import com.thoughtstream.aepf.handlers.EventSourcerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
@SpringBootApplication
public class AsyncEventsProcessorTestImpl implements CommandLineRunner {

    @Value("${zookeeper.connectionStr}")
    private String zkConStr;

    @Autowired
    private AsyncEventsProcessor<IndexBasedEvent> processor;

    public static void main(String[] args) {
        SpringApplication.run(AsyncEventsProcessorTestImpl.class, args);
    }

    @Bean
    public AsyncEventsProcessor<IndexBasedEvent> createProcessor() {
        List<EventSourcerFactory<IndexBasedEvent>> eventSourcerFactories = new LinkedList<>();
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer1"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer2"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer3"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer4"));
        eventSourcerFactories.add(new IndexBasedEventSourcerFactory("Sourcer5"));

        IndexBasedEventSerializerDeserializer indexBasedEventSerializerDeserializer = new IndexBasedEventSerializerDeserializer();
        IndexBasedShardKeyProvider shardKeyProvider = new IndexBasedShardKeyProvider();
        IndexBasedEventHandler indexBasedEventHandler = new IndexBasedEventHandler();
        return new AsyncEventsProcessor<>(zkConStr, "/rs", "aepf-test", eventSourcerFactories,
                indexBasedEventSerializerDeserializer, shardKeyProvider, indexBasedEventHandler, 50, false);
    }

    @Override
    public void run(String... args) throws Exception {
        processor.start();

        Thread.sleep(600000000); //FIXME:
    }
}
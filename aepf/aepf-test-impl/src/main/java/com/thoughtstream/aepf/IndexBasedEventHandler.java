package com.thoughtstream.aepf;

import com.thoughtstream.aepf.handlers.EventHandler;
import kamon.Kamon;
import kamon.metric.CounterMetric;
import kamon.metric.MeasurementUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class IndexBasedEventHandler implements EventHandler<IndexBasedEvent> {
    private static final Logger log = LoggerFactory.getLogger(AsyncEventsProcessor.class);
    private static CounterMetric counter = Kamon.counter("index_based_requests");

    @Override
    public void process(IndexBasedEvent event) {
        log.info("Processing %s", event);
        exec(() -> Thread.sleep(2000));
        log.info("Finished processing %s", event);
        counter.increment();
    }
}

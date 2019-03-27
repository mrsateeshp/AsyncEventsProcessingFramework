package com.thoughtstream.aepf.testimpl;

import com.thoughtstream.aepf.AsyncEventsProcessor;
import com.thoughtstream.aepf.handlers.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thoughtstream.aepf.DefaultConstants.exec;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class IndexBasedEventHandler implements EventHandler<IndexBasedEvent> {
    private static final Logger log = LoggerFactory.getLogger(AsyncEventsProcessor.class);

    @Override
    public void process(IndexBasedEvent event) {
        log.info("Processing %s", event);
        exec(() -> Thread.sleep(2000));
        log.info("Finished processing %s", event);
    }
}

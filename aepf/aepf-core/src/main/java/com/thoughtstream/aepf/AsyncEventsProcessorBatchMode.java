package com.thoughtstream.aepf;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.handlers.EventHandler;
import com.thoughtstream.aepf.handlers.EventSourcerBatchMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Sateesh Pinnamaneni
 * @since 11/01/2018
 */
public class AsyncEventsProcessorBatchMode<T extends Event> {
    private static final Logger log = LoggerFactory.getLogger(AsyncEventsProcessorBatchMode.class);

    private final Collection<EventSourcerBatchMode<T>> eventSourcers;
    private final EventHandler<T> eventHandler;

    public AsyncEventsProcessorBatchMode(Collection<EventSourcerBatchMode<T>> eventSourcers, EventHandler<T> eventHandler) {
        this.eventSourcers = eventSourcers;
        this.eventHandler = eventHandler;
    }

    public synchronized void stop() {
    }

    public synchronized void start() {
        try {
            for (EventSourcerBatchMode<T> eventSourcer : eventSourcers) {
                String sourceId = eventSourcer.getSourceId();
                log.info("Starting the event sourcer: {}", sourceId);
                while (eventSourcer.hasNext()) {
                    T event = eventSourcer.nextEvent();
                    log.info("[RECEIVED]Processing: {}", event);
                    eventHandler.process(event);
                    log.info("[PROCESSED]Finished processing: {}", event);
                }
                log.info("Completed the event sourcer: {}", sourceId);
            }
        } catch (Exception e) {
            stop();
        }

    }
}

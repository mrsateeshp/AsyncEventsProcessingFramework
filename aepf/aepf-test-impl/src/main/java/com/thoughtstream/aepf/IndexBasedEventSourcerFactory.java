package com.thoughtstream.aepf;

import com.thoughtstream.aepf.handlers.EventSourcer;
import com.thoughtstream.aepf.handlers.EventSourcerFactory;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public class IndexBasedEventSourcerFactory implements EventSourcerFactory<IndexBasedEvent> {
    private final String eventSourceId;
    private static final IndexBasedEvent DEFAULT_START_EVENT = new IndexBasedEvent(-1);

    public IndexBasedEventSourcerFactory(String eventSourceId) {
        this.eventSourceId = eventSourceId;
    }

    @Override
    public String getSourceId() {
        return eventSourceId;
    }

    @Override
    public EventSourcer<IndexBasedEvent> create(IndexBasedEvent from) {
        if (from == null) {
            return new IndexBasedEventSourcer(eventSourceId, DEFAULT_START_EVENT);
        }
        return new IndexBasedEventSourcer(eventSourceId, from);
    }
}

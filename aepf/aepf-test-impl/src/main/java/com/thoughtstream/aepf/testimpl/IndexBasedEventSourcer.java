package com.thoughtstream.aepf.testimpl;

import com.thoughtstream.aepf.handlers.EventSourcer;

import java.util.Optional;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public class IndexBasedEventSourcer implements EventSourcer<IndexBasedEvent> {
    private final String eventSourceId;
    private IndexBasedEvent currentEvent;

    public IndexBasedEventSourcer(String eventSourceId, IndexBasedEvent from) {
        this.eventSourceId = eventSourceId;
        this.currentEvent = from;
    }

    @Override
    public String getSourceId() {
        return eventSourceId;
    }

    @Override
    public Optional<IndexBasedEvent> nextEvent(long maxWait) {
        currentEvent = new IndexBasedEvent(this.currentEvent.getIndex() < Integer.MAX_VALUE ? this.currentEvent.getIndex() + 1 : 0);
        return Optional.of(currentEvent);
    }

    @Override
    public void close() {

    }
}

package com.thoughtstream.aepf.beans;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class QueuedEvent<T extends Event> {
    private final String eventSourceId;
    private final T event;

    public QueuedEvent(String eventSourceId, T event) {
        this.eventSourceId = eventSourceId;
        this.event = event;
    }

    public String getEventSourceId() {
        return eventSourceId;
    }

    public T getEvent() {
        return event;
    }

    @Override
    public String toString() {
        return "QueuedEvent{" +
                "eventSourceId='" + eventSourceId + '\'' +
                ", event=" + event +
                '}';
    }
}

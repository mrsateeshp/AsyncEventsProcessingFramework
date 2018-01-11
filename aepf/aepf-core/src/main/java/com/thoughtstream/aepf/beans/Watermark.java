package com.thoughtstream.aepf.beans;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Sateesh Pinnamaneni
 * @since 11/12/2017
 */
public class Watermark<T extends Event> {
    private T lastEvent;
    private LinkedHashMap<String, LinkedList<T>> outstandingEvents;

    public Watermark() {
        this(null, null);
    }

    public Watermark(T lastEvent, LinkedHashMap<String, LinkedList<T>> outstandingEvents) {
        this.lastEvent = lastEvent;
        this.outstandingEvents = outstandingEvents == null ? new LinkedHashMap<>() :
                outstandingEvents.entrySet().stream() //deep cloning the collection
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new LinkedList<>(e.getValue()), (a, b) -> a, LinkedHashMap<String, LinkedList<T>>::new));
    }

    public T getLastEvent() {
        return lastEvent;
    }

    public LinkedHashMap<String, LinkedList<T>> getOutstandingEvents() {
        return outstandingEvents;
    }

    public void setLastEvent(T lastEvent) {
        this.lastEvent = lastEvent;
    }

    public void setOutstandingEvents(LinkedHashMap<String, LinkedList<T>> outstandingEvents) {
        this.outstandingEvents = outstandingEvents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Watermark<?> watermark = (Watermark<?>) o;

        if (lastEvent != null ? !lastEvent.equals(watermark.lastEvent) : watermark.lastEvent != null) return false;
        return outstandingEvents != null ? outstandingEvents.equals(watermark.outstandingEvents) : watermark.outstandingEvents == null;

    }

    @Override
    public int hashCode() {
        int result = lastEvent != null ? lastEvent.hashCode() : 0;
        result = 31 * result + (outstandingEvents != null ? outstandingEvents.hashCode() : 0);
        return result;
    }
}

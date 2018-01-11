package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;

/**
 * @author Sateesh Pinnamaneni
 * @since 11/01/2018
 */
public interface EventSourcerBatchMode<T extends Event> {
    String getSourceId();

    T nextEvent();

    boolean hasNext();

    void close();
}

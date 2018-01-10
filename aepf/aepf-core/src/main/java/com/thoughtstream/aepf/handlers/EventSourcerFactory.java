package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
//FIXME: better name - should be a prototype factory
public interface EventSourcerFactory<T extends Event> {
    String getSourceId();

    /**
     *
     * @param from - null indicates default starting point
     * @return
     */
    EventSourcer<T> create(T from);
}

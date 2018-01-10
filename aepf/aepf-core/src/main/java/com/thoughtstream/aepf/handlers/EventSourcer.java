package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;

import java.util.Optional;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public interface EventSourcer<T extends Event> {
    String getSourceId();

    /**
     * this is a blocking call
     *
     * @return
     */
    Optional<T> nextEvent(long maxWait);

    void close();
}

package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public interface EventHandler<T extends Event> {
    void process(T event);
}

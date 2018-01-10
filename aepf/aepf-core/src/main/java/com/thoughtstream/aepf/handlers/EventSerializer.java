package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public interface EventSerializer<T extends Event> {
    String serialize(T event);
}

package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public interface ShardKeyProvider<T extends Event> {
    String getShardKey(T event);
}

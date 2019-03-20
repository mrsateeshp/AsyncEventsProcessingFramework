package com.thoughtstream.aepf.testimpl;

import com.thoughtstream.aepf.handlers.ShardKeyProvider;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class IndexBasedShardKeyProvider implements ShardKeyProvider<IndexBasedEvent> {
    @Override
    public String getShardKey(IndexBasedEvent event) {
        String prefix = event.getIndex() % 2 == 0 ? "EVEN" : "ODD";
        return prefix + event.getIndex() % 10;
    }
}

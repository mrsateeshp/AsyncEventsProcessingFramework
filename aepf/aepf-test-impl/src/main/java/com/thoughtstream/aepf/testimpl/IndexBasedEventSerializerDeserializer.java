package com.thoughtstream.aepf.testimpl;

import com.thoughtstream.aepf.handlers.EventSerializerDeserializer;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public class IndexBasedEventSerializerDeserializer implements EventSerializerDeserializer<IndexBasedEvent> {
    @Override
    public IndexBasedEvent deserialize(String event) {
        return new IndexBasedEvent(Integer.parseInt(event));
    }

    @Override
    public String serialize(IndexBasedEvent event) {
        return String.valueOf(event.getIndex());
    }
}

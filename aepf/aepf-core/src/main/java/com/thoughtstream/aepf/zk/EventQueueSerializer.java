package com.thoughtstream.aepf.zk;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.QueuedEvent;
import com.thoughtstream.aepf.handlers.EventSerializerDeserializer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;

/**
 * @author Sateesh Pinnamaneni
 * @since 05/01/2018
 */
public class EventQueueSerializer<T extends Event> implements QueueSerializer<QueuedEvent<T>> {
    private static final String DELIMITER = "==";
    private final EventSerializerDeserializer<T> eventSerializerDeserializer;

    public EventQueueSerializer(EventSerializerDeserializer<T> eventSerializerDeserializer) {
        this.eventSerializerDeserializer = eventSerializerDeserializer;
    }

    @Override
    public byte[] serialize(QueuedEvent<T> item) {
        return (item.getEventSourceId() + DELIMITER + eventSerializerDeserializer.serialize(item.getEvent())).getBytes(DEFAULT_CHAR_SET);
    }

    @Override
    public QueuedEvent<T> deserialize(byte[] bytes) {
        String[] split = new String(bytes, DEFAULT_CHAR_SET).split(DELIMITER);
        return new QueuedEvent<>(split[0], eventSerializerDeserializer.deserialize(split[1]));
    }
}

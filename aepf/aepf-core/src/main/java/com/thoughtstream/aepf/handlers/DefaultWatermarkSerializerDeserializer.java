package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.Watermark;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
//FIXME: encode: =, ||, newLine(\n)
public class DefaultWatermarkSerializerDeserializer<T extends Event> implements WatermarkSerializerDeserializer<T> {
    private static final String EVENTS_DELIMITER = "||";
    private static final String EVENTS_DELIMITER_REGEX = "\\|\\|";
    private static final String KV_DELIMITER = "==";
    private static final String KV_DELIMITER_REGEX = "\\=\\=";

    private final EventSerializerDeserializer<T> serDeser;

    public DefaultWatermarkSerializerDeserializer(EventSerializerDeserializer<T> serDeser) {
        this.serDeser = serDeser;
    }

    @Override
    public Watermark<T> deserialize(byte[] watermarkBytes) {
        String watermarkStr = new String(watermarkBytes);
        if (watermarkStr.equals(WATERMARK_INIT_STR)) {
            return new Watermark<>(null, null);
        }

        String[] lines = watermarkStr.split("\\n");
        if (lines.length < 1) {
            //FIXME:
            throw new RuntimeException("Initialization failed!");
        }
        String lastEventString = lines[0];
        T lastEvent = serDeser.deserialize(lastEventString);
        LinkedHashMap<String, LinkedList<T>> outstandingEvents = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            LinkedList<T> events = new LinkedList<>();
            String[] kv = lines[i].split(KV_DELIMITER_REGEX);
            if (kv.length != 2) {
                //FIXME:
                throw new RuntimeException("Initialization failed!");
            }

            String shardKey = kv[0];
            for (String event : kv[1].split(EVENTS_DELIMITER_REGEX)) {
                events.add(serDeser.deserialize(event));
            }

            outstandingEvents.put(shardKey, events);
        }

        return new Watermark<>(lastEvent, outstandingEvents);
    }

    @Override
    public byte[] serialize(Watermark<T> watermark) {
        if (watermark.getLastEvent() == null) {
            return WATERMARK_INIT_STR.getBytes(DEFAULT_CHAR_SET);
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(serDeser.serialize(watermark.getLastEvent()));
        for (Map.Entry<String, LinkedList<T>> kv : watermark.getOutstandingEvents().entrySet()) {
            String shardKey = kv.getKey();
            LinkedList<T> events = kv.getValue();
            stringBuilder.append("\n");
            stringBuilder.append(shardKey);
            stringBuilder.append(KV_DELIMITER);

            stringBuilder.append(events.stream().map(serDeser::serialize).collect(Collectors.joining(EVENTS_DELIMITER)));
        }

        return stringBuilder.toString().getBytes(DEFAULT_CHAR_SET);
    }
}

package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.Watermark;

import java.util.*;
import java.util.stream.Collectors;

import static com.thoughtstream.aepf.DefaultConstants.DEFAULT_CHAR_SET;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public class DefaultWatermarkSerializerDeserializer<T extends Event> implements WatermarkSerializerDeserializer<T> {
    private static final String EVENTS_DELIMITER = "||";
    private static final String EVENTS_DELIMITER_REGEX = "\\|\\|(?!\\|)";
    private static final String KV_DELIMITER = "==";
    private static final String KV_DELIMITER_REGEX = "==(?!=)";

    private final EncodedEventSerializerDeserializer serDeser;

    public DefaultWatermarkSerializerDeserializer(EventSerializerDeserializer<T> serDeser) {
        this.serDeser = new EncodedEventSerializerDeserializer(serDeser);
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

            String shardKey = serDeser.decode(kv[0]);
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
            String shardKey = serDeser.encode(kv.getKey());

            stringBuilder.append("\n");
            stringBuilder.append(shardKey);
            stringBuilder.append(KV_DELIMITER);

            LinkedList<T> events = kv.getValue();
            stringBuilder.append(events.stream().map(serDeser::serialize).collect(Collectors.joining(EVENTS_DELIMITER)));
        }

        return stringBuilder.toString().getBytes(DEFAULT_CHAR_SET);
    }

    private class EncodedEventSerializerDeserializer implements EventSerializerDeserializer<T> {
        private final LinkedHashMap<String, String> ENCODE_MAP = encodeMap();
        private final LinkedHashMap<String, String> ENCODE_MAP_REVERSE_ORDER = reverseEncodeMap();
        private final EventSerializerDeserializer<T> org;

        private EncodedEventSerializerDeserializer(EventSerializerDeserializer<T> org) {
            this.org = org;
        }

        private LinkedHashMap<String, String> encodeMap() {
            LinkedHashMap<String, String> orderedMap = new LinkedHashMap<>();
            orderedMap.put("=", "\\=");
            orderedMap.put("|", "\\|");
            orderedMap.put("+", "\\+");
            orderedMap.put("\n", "\\++");

            return orderedMap;
        }

        private LinkedHashMap<String, String> reverseEncodeMap() {
            LinkedHashMap<String, String> map = encodeMap();
            ArrayList<String> list = new ArrayList<>(map.keySet());
            Collections.reverse(list);

            LinkedHashMap<String, String> orderedMap = new LinkedHashMap<>();
            for (String s : list) {
                orderedMap.put(s, map.get(s));
            }

            return orderedMap;
        }

        private String encode(final String input) {
            String encoded = input;
            for (Map.Entry<String, String> kv : ENCODE_MAP.entrySet()) {
                encoded = encoded.replace(kv.getKey(), kv.getValue());
            }

            return encoded;
        }

        private String decode(final String input) {
            String encoded = input;
            for (Map.Entry<String, String> kv : ENCODE_MAP_REVERSE_ORDER.entrySet()) {
                encoded = encoded.replace(kv.getValue(), kv.getKey());
            }

            return encoded;
        }


        @Override
        public T deserialize(String event) {
            return org.deserialize(decode(event));
        }

        @Override
        public String serialize(T event) {
            return encode(org.serialize(event));
        }
    }

}

package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.Watermark;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import static org.junit.Assert.*;

/**
 * @author Sateesh Pinnamaneni
 * @since 11/01/2018
 */
public class DefaultWatermarkSerializerDeserializerTest {
    private DefaultWatermarkSerializerDeserializer<StringEvent> serializerDeserializer;

    @Before
    public void setUp() throws Exception {
        serializerDeserializer = new DefaultWatermarkSerializerDeserializer<>(new EventSerializerDeserializer<StringEvent>() {
            @Override
            public StringEvent deserialize(String event) {
                return new StringEvent(event);
            }

            @Override
            public String serialize(StringEvent event) {
                return event.str;
            }
        });

    }

    @Test
    public void serializeAndDeserialize() throws Exception {
        LinkedHashMap<String, LinkedList<StringEvent>> outstanding = new LinkedHashMap<>();
        outstanding.put("+=\n+=MIDDLE+=\n+=", new LinkedList<>(Arrays.asList(
                new StringEvent("LastEventWith==String"),
                new StringEvent("LastEventWith=String"),
                new StringEvent("LastEventWith||String"),
                new StringEvent("LastEventWith|String"),
                new StringEvent("LastEventWith++String"),
                new StringEvent("LastEventWith+String"),
                new StringEvent("LastEventWith\n\nString"),
                new StringEvent("LastEventWith\nString"))));

        outstanding.put("+=\n+=START+=\n+=", new LinkedList<>(Arrays.asList(
                new StringEvent("==LastEventWithString"),
                new StringEvent("=LastEventWithString"),
                new StringEvent("||LastEventWithString"),
                new StringEvent("|LastEventWithString"),
                new StringEvent("++LastEventWithString"),
                new StringEvent("+LastEventWithString"),
                new StringEvent("\n\nLastEventWithString"),
                new StringEvent("\nLastEventWithString"))));


        outstanding.put("+=\n+=END+=\n+=", new LinkedList<>(Arrays.asList(
                new StringEvent("LastEventWithString=="),
                new StringEvent("LastEventWithString="),
                new StringEvent("LastEventWithString||"),
                new StringEvent("LastEventWithString|"),
                new StringEvent("LastEventWithString++"),
                new StringEvent("LastEventWithString+"),
                new StringEvent("LastEventWithString\n\n"),
                new StringEvent("LastEventWithString\n"))));

        outstanding.put("+=\n+=ALL+=\n+=", new LinkedList<>(Arrays.asList(
                new StringEvent("==LastEventWith==String=="),
                new StringEvent("=LastEventWith=String="),
                new StringEvent("||LastEventWith||String||"),
                new StringEvent("|LastEventWith|String|"),
                new StringEvent("++LastEventWith++String++"),
                new StringEvent("+LastEventWith+String+"),
                new StringEvent("\n\nLastEventWith\n\nString\n\n"),
                new StringEvent("\nLastEventWith\nString\n"))));

        Watermark<StringEvent> watermark = new Watermark<>(new StringEvent("LastEventWith||String||"), outstanding);
        byte[] bytes = serializerDeserializer.serialize(watermark);
        Watermark<StringEvent> deserializedWatermark = serializerDeserializer.deserialize(bytes);
        Assert.assertEquals(watermark, deserializedWatermark);
    }

    private class StringEvent implements Event {
        final String str;

        private StringEvent(String str) {
            this.str = str;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringEvent that = (StringEvent) o;

            return str != null ? str.equals(that.str) : that.str == null;

        }

        @Override
        public int hashCode() {
            return str != null ? str.hashCode() : 0;
        }
    }
}
package com.thoughtstream.aepf.handlers;

import com.thoughtstream.aepf.beans.Event;
import com.thoughtstream.aepf.beans.Watermark;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public interface WatermarkSerializerDeserializer<T extends Event> {
    String WATERMARK_INIT_STR = "INIT";

    Watermark<T> deserialize(byte[] watermarkBytes);

    byte[] serialize(Watermark<T> watermark);
}

package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

public class TradeDeserializer implements Deserializer<Trade>, Serializable {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Trade deserialize(String topic, byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int ticker = buf.getInt();
        long time = buf.getLong();
        int price = buf.getInt();
        int quantity = buf.getInt();
        return new Trade(time, ticker, quantity, price);
    }

    @Override
    public void close() {
    }
}

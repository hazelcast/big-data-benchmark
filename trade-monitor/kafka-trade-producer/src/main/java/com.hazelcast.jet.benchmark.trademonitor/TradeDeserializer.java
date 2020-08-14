package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class TradeDeserializer implements Deserializer<Trade>, Serializable {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Trade deserialize(String topic, byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            String ticker = in.readUTF();
            long time = in.readLong();
            int price = in.readInt();
            int quantity = in.readInt();
            return new Trade(time, ticker, quantity, price);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }
}

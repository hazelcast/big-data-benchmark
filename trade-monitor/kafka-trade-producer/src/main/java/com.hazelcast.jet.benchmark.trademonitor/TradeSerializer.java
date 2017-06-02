package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class TradeSerializer implements Serializer<Trade> {
    private final ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Trade trade) {
        DataOutputStream out = new DataOutputStream(outStream);
        try {
            out.writeUTF(trade.getTicker());
            out.writeLong(trade.getTime());
            out.writeInt(trade.getPrice());
            out.writeInt(trade.getQuantity());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] bytes = outStream.toByteArray();
        outStream.reset();
        return bytes;
    }

    @Override
    public void close() {
        try {
            outStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

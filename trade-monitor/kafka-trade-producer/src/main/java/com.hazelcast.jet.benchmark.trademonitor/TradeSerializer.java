package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class TradeSerializer implements Serializer<Trade> {
    private final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    private final DataOutputStream dataOut = new DataOutputStream(outStream);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Trade trade) {
        try {
            dataOut.writeUTF(trade.getTicker());
            dataOut.writeLong(trade.getTime());
            dataOut.writeInt(trade.getPrice());
            dataOut.writeInt(trade.getQuantity());
            dataOut.flush();
            return outStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize a Trade object", e);
        } finally {
            outStream.reset();
        }
    }

    @Override
    public void close() {
    }
}

package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class TradeSerializer implements Serializer<Trade> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Trade trade) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES);
        buf.putInt(trade.getTicker());
        buf.putLong(trade.getTime());
        buf.putInt(trade.getPrice());
        buf.putInt(trade.getQuantity());
        return buf.array();
    }

    @Override
    public void close() {
    }
}

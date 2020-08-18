package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.benchmark.Trade;
import com.hazelcast.jet.benchmark.Util;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaTradeSerializer implements Serializer<Trade> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Trade trade) {
        return Util.serializeTrade(trade);
    }

    @Override
    public void close() {
    }
}

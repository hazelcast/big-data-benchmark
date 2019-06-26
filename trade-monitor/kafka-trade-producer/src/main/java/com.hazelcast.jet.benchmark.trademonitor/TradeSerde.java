package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TradeSerde implements Serde<Trade> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Trade> serializer() {
        return new TradeSerializer();
    }

    @Override
    public Deserializer<Trade> deserializer() {
        return new TradeDeserializer();
    }
}

package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.benchmark.Trade;
import com.hazelcast.jet.benchmark.Util;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Map;

public class KafkaTradeDeserializer implements Deserializer<Trade>, Serializable {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Trade deserialize(String topic, byte[] bytes) {
        return Util.deserializeTrade(bytes);
    }

    @Override
    public void close() {
    }
}

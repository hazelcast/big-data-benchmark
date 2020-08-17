package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.hazelcast.jet.benchmark.trademonitor.Trade.TICKER_MAX_LENGTH;
import static com.hazelcast.jet.benchmark.trademonitor.TradeSerializer.PADDING_BYTE;

public class TradeDeserializer implements Deserializer<Trade>, Serializable {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Trade deserialize(String topic, byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int tickerLen = 0;
        while (bytes[tickerLen] != PADDING_BYTE && tickerLen < TICKER_MAX_LENGTH) {
            tickerLen++;
        }
        String ticker = new String(bytes, 0, tickerLen);
        long time = buf.getLong();
        int price = buf.getInt();
        int quantity = buf.getInt();
        return new Trade(time, ticker, quantity, price);
    }

    @Override
    public void close() {
    }
}

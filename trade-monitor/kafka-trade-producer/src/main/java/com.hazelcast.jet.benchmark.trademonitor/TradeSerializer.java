package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.hazelcast.jet.benchmark.trademonitor.Trade.TICKER_MAX_LENGTH;

public class TradeSerializer implements Serializer<Trade> {
    static final byte PADDING_BYTE = (byte) ' ';

    private final byte[] tickerBuf = new byte[5];

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Trade trade) {
        ByteBuffer buf = ByteBuffer.allocate(TICKER_MAX_LENGTH + Long.BYTES + Integer.BYTES + Integer.BYTES);
        String ticker = trade.getTicker();
        if (ticker.length() > TICKER_MAX_LENGTH) {
            throw new RuntimeException("This ticker is too long: " + ticker + ". Max length is " + TICKER_MAX_LENGTH);
        }
        int i = 0;
        for (; i < ticker.length(); i++) {
            tickerBuf[i] = (byte) ticker.charAt(i);
        }
        for (; i < TICKER_MAX_LENGTH; i++) {
            tickerBuf[i] = PADDING_BYTE;
        }
        buf.put(tickerBuf);
        buf.putLong(trade.getTime());
        buf.putInt(trade.getPrice());
        buf.putInt(trade.getQuantity());
        return buf.array();
    }

    @Override
    public void close() {
    }
}

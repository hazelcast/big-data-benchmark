package com.hazelcast.jet.benchmark.nexmark.model;

public class Auction extends Event {
    private final long sellerId;

    public Auction(long id, long timestamp, long sellerId) {
        super(id, timestamp);
        this.sellerId = sellerId;
    }

    public long sellerId() {
        return sellerId;
    }
}

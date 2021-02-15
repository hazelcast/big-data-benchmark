package com.hazelcast.jet.benchmark.nexmark.model;

public class Auction extends Event {
    private final long sellerId;

    private final int category;

    public Auction(long id, long timestamp, long sellerId, int category) {
        super(id, timestamp);
        this.sellerId = sellerId;
        this.category = category;
    }

    public long sellerId() {
        return sellerId;
    }

    public int category() {
        return category;
    }
}

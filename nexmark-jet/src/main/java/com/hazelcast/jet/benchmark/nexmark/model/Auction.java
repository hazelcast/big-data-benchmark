package com.hazelcast.jet.benchmark.nexmark.model;

public class Auction extends Event {
    private final long sellerId;

    private final int category;
    private final long expires;

    public Auction(long id, long timestamp, long sellerId, int category, long expires) {
        super(id, timestamp);
        this.sellerId = sellerId;
        this.category = category;
        this.expires = expires;
    }

    public long sellerId() {
        return sellerId;
    }

    public int category() {
        return category;
    }

    public long expires() {
        return expires;
    }
}

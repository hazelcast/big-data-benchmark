package com.hazelcast.jet.benchmark.nexmark.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class Bid {
    private final long seq;
    private final long timestamp;
    private final long auctionId;
    private final long price;

    public Bid(long seq, long timestamp, long auctionId, long price) {
        this.seq = seq;
        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.price = price;
    }

    public long seq() {
        return seq;
    }

    public long timestamp() {
        return timestamp;
    }

    public long auctionId() {
        return auctionId;
    }

    public long price() {
        return price;
    }

    public static class BidSerializer implements StreamSerializer<Bid> {

        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void write(ObjectDataOutput out, Bid bid) throws IOException {
            out.writeLong(bid.seq());
            out.writeLong(bid.timestamp());
            out.writeLong(bid.auctionId());
            out.writeLong(bid.price());
        }

        @Override
        public Bid read(ObjectDataInput in) throws IOException {
            long seq = in.readLong();
            long timestamp = in.readLong();
            long auctionId = in.readLong();
            long price = in.readLong();
            return new Bid(seq, timestamp, auctionId, price);
        }
    }
}

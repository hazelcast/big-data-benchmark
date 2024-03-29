package com.example.jet.benchmark.nexmark;

import com.example.jet.benchmark.nexmark.model.Auction;
import com.example.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;

/**
 * A processor that ingests a merged stream of {@link Auction} and {@link
 * Bid} items. It tracks the bids and emits the winning bid when the
 * auction expires.
 */
public class JoinAuctionToWinningBidP extends AbstractProcessor {

    private final long auctionMaxDuration;

    /**
     * Key: expiry time
     * Value: auctions that expire at that time
     */
    private final TreeMap<Long, List<Auction>> expiryTimeToAuction = new TreeMap<>();

    /**
     * Maximum bids for auctions. We track bids received before the auction is
     * received, we also track bids with timestamp after the auction expiry -
     * those need to be cleaned up regularly.
     */
    private final Map<Long, Bid> auctionIdToMaxBid = new HashMap<>();

    private Traverser<Object> outputTraverser;
    private long nextCleanUpTime;
    private long watermarkTimestamp;

    public static FunctionEx<StreamStage<Object>, StreamStage<Tuple2<Auction, Bid>>> joinAuctionToWinningBid(
            long auctionMaxDuration
    ) {
        return upstream -> upstream
                .groupingKey(o -> o instanceof Bid ? ((Bid) o).auctionId() : ((Auction) o).id())
                .customTransform("findClosedAuctions", () -> new JoinAuctionToWinningBidP(auctionMaxDuration));
    }

    private JoinAuctionToWinningBidP(long auctionMaxDuration) {
        this.auctionMaxDuration = auctionMaxDuration;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object o) {
        JetEvent<?> jetEvent = (JetEvent<?>) o;
        if (jetEvent.timestamp() < watermarkTimestamp) {
            // drop late event
            return true;
        }
        Object item = jetEvent.payload();
        if (item instanceof Auction) {
            Auction auction = (Auction) item;
            expiryTimeToAuction
                    .computeIfAbsent(auction.expires(), x -> new ArrayList<>())
                    .add(auction);
        } else {
            Bid bid = (Bid) item;
            auctionIdToMaxBid.merge(bid.auctionId(), bid, (prev, curr) -> prev.price() >= curr.price() ? prev : curr);
        }
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        watermarkTimestamp = watermark.timestamp();
        if (outputTraverser == null) {
            Collection<List<Auction>> expiredAuctions = expiryTimeToAuction.headMap(watermarkTimestamp + 1)
                                                                           .values();
            outputTraverser = traverseIterable(expiredAuctions)
                    .flatMap(Traversers::traverseIterable)
                    .map(auction -> tuple2(auction, auctionIdToMaxBid.remove(auction.id())))
                    .filter(t2 -> t2.f1() != null)
                    .map(t2 -> (Object) jetEvent(t2.f0().expires(), t2))
                    .append(watermark)
                    .onFirstNull(() -> {
                        expiredAuctions.clear();
                        outputTraverser = null;
                    });

            // clean up old bids received after the auction expired if sufficient time elapsed
            if (nextCleanUpTime <= watermarkTimestamp) {
                nextCleanUpTime = watermarkTimestamp + auctionMaxDuration / 8;
                auctionIdToMaxBid.values()
                                 .removeIf(bid -> bid.timestamp() < watermarkTimestamp - auctionMaxDuration);
            }
        }
        return emitFromTraverser(outputTraverser);
    }
}

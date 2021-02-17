package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
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
    private final TreeMap<Long, List<Auction>> auctionsByExpiry = new TreeMap<>();

    /**
     * Maximum bids for auctions. We track bids received before the auction is
     * received, we also track bids with timestamp after the auction expiry -
     * those need to be cleaned up regularly.
     *
     * Key: auction ID
     * Value: maximum bid
     */
    private final Map<Long, Bid> bids = new HashMap<>();

    private Traverser<Tuple2<Auction, Bid>> outputTraverser;
    private long nextCleanUpTime;

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
    protected boolean tryProcess0(@Nonnull Object item) {
        if (item instanceof Auction) {
            Auction auction = (Auction) item;
            auctionsByExpiry
                    .computeIfAbsent(auction.expires(), x -> new ArrayList<>())
                    .add(auction);
        } else {
            Bid bid = (Bid) item;
            bids.merge(bid.auctionId(), bid, (prev, curr) -> prev.price() >= curr.price() ? prev : curr);
        }
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        if (outputTraverser == null) {
            Collection<List<Auction>> expiredAuctions = auctionsByExpiry.headMap(watermark.timestamp()).values();
            outputTraverser = traverseIterable(expiredAuctions)
                    .flatMap(Traversers::traverseIterable)
                    .map(auction -> tuple2(auction, bids.remove(auction.id())))
                    .onFirstNull(() -> outputTraverser = null);

            // clean up old bids received after the auction expired if sufficient time elapsed
            if (nextCleanUpTime <= watermark.timestamp()) {
                nextCleanUpTime = watermark.timestamp() + auctionMaxDuration / 8;
                bids.values().removeIf(bid -> bid.timestamp() < watermark.timestamp() - auctionMaxDuration);
            }
        }
        return emitFromTraverser(outputTraverser);
    }
}

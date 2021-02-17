package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;

public class Q04CategoryAvg extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int auctionIdModulus = 128;
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = Math.max(1, eventsPerSecond / (8192 * auctionIdModulus));
        int auctionBidRatio = 10;
        long auctionMaxDuration = 1024;

        // We generate auctions at rate eventsPerSecond / auctionBidRatio.
        // We generate bids at rate eventsPerSecond, each bid refers to auctionId = seq / auctionBidRatio

        StreamStage<Object> auctions = pipeline
                .readFrom(EventSourceP.eventSource(eventsPerSecond / auctionBidRatio, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = getRandom(271 * seq, auctionMaxDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0)
                .map(p -> p);

        StreamStage<Object> bids = pipeline
                .readFrom(EventSourceP.eventSource(eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> new Bid(seq, timestamp, seq / auctionBidRatio, 0)))
                .withNativeTimestamps(0)
                .map(p -> p);

        return auctions
                .merge(bids)
                .groupingKey(o -> o instanceof Bid ? ((Bid) o).auctionId() : ((Auction) o).id())
                .<Tuple3<Integer, Long, Long>>customTransform("findClosedAuctions", () -> new ClosedAuctionP(auctionMaxDuration))
                .groupingKey(Tuple3::f0) // Tuple 3 is: category, maxPrice, closeTimestamp
                .rollingAggregate(maxBy(comparing(Tuple3::f1)))
                .map(Entry::getValue)

                .filter(t3 -> t3.f2() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, Tuple3::f2));
    }

    /**
     * A processor that ingests a merged stream of {@link Auction} and {@link
     * Bid} items. It tracks the bids and emits the winning bid when the
     * auction expires.
     */
    private static final class ClosedAuctionP extends AbstractProcessor {

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

        private Traverser<Tuple3<Integer, Long, Long>> outputTraverser;
        private long nextCleanUpTime;

        public ClosedAuctionP(long auctionMaxDuration) {
            this.auctionMaxDuration = auctionMaxDuration;
        }

        @Override
        protected boolean tryProcess0(Object item) {
            if (item instanceof Auction) {
                Auction auction = (Auction) item;
                auctionsByExpiry
                        .computeIfAbsent(auction.expires(), x -> new ArrayList<>())
                        .add(auction);
            } else {
                Bid bid = (Bid) item;
                bids.merge(bid.auctionId(), bid, (bid1, bid2) -> bid1.price() > bid2.price() ? bid1 : bid2);
            }
            return true;
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            if (outputTraverser == null) {
                Collection<List<Auction>> expiredAuctions = auctionsByExpiry.headMap(watermark.timestamp()).values();
                outputTraverser = traverseIterable(expiredAuctions)
                        .flatMap(Traversers::traverseIterable)
                        .map(auction -> {
                            Bid bid = bids.remove(auction.id());
                            return bid != null ? tuple3(auction.category(), bid.price(), watermark.timestamp()) : null;
                        })
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
}

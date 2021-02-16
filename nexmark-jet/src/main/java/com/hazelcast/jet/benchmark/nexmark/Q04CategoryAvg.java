package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.function.ComparatorEx.comparing;
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
                .mapStateful(
                        auctionMaxDuration,
                        AggrBuffer::new,
                        AggrBuffer::process,
                        (buf, key, result) -> null)
                .groupingKey(Tuple3::f0) // Tuple 3 is: category, maxPrice, timestamp
                .rollingAggregate(maxBy(comparing(Tuple3::f1)))
                .map(Entry::getValue)

                .filter(t3 -> t3.f2() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, Tuple3::f2));
    }

    private static final class AggrBuffer implements Serializable {

        private Auction auction;
        private long maxPrice = Long.MIN_VALUE;

        public Tuple3<Integer, Long, Long> process(Long key, Object item) {
            if (item instanceof Auction) {
                auction = (Auction) item;
                if (maxPrice > Long.MIN_VALUE) {
                    return tuple3(auction.category(), maxPrice, auction.timestamp());
                }
            } else {
                Bid bid = (Bid) item;
                if (auction != null && bid.timestamp() < auction.expires() && bid.price() > maxPrice) {
                    maxPrice = bid.price();

                    return tuple3(auction.category(), maxPrice, bid.timestamp());
                }
            }
            return null;
        }
    }
}

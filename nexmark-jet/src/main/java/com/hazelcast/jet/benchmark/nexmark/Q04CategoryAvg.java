package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.benchmark.nexmark.JoinAuctionToWinningBidP.joinAuctionToWinningBid;
import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;


public class Q04CategoryAvg extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int bidsPerAuction = 10;
        long auctionMaxDuration = 2L * numDistinctKeys * bidsPerAuction * 1000 / eventsPerSecond;

        // We generate auctions at rate eventsPerSecond / bidsPerAuction.
        // We generate bids at rate eventsPerSecond, each bid refers to
        // auctionId = seq / bidsPerAuction

        StreamStage<Object> auctions = pipeline
                .readFrom(eventSource(eventsPerSecond / bidsPerAuction, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = getRandom(271 * seq, auctionMaxDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0)
                .map(p -> p);

        StreamStage<Object> bids = pipeline
                .readFrom(eventSource(eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> new Bid(seq, timestamp, seq / bidsPerAuction, 0)))
                .withNativeTimestamps(0)
                .map(p -> p);

        return auctions
                .merge(bids)
                .apply(joinAuctionToWinningBid(auctionMaxDuration))
                .map(t -> tuple3(t.f0().category(), t.f1().price(), t.f0().expires()))
                .groupingKey(Tuple3::f0) // Tuple 3 is: category, maxPrice, expireTime
                .rollingAggregate(maxBy(comparing(Tuple3::f1)))
                .map(Entry::getValue)

                .apply(stage -> determineLatency(stage, Tuple3::f2));
    }
}

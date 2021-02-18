package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.jet.benchmark.nexmark.JoinAuctionToWinningBidP.joinAuctionToWinningBid;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.lang.Math.max;


public class Q04AveragePriceForCategory extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int bidsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int auctionsPerSecond = 1000;
        int bidsPerAuction = max(1, bidsPerSecond / auctionsPerSecond);
        long auctionMinDuration = (long) numDistinctKeys * bidsPerAuction * 1000 / bidsPerSecond;
        long auctionMaxDuration = 2 * auctionMinDuration;
        System.out.format("Auction duration: %,d .. %,d ms%n", auctionMinDuration, auctionMaxDuration);

        // We generate auctions at rate bidsPerSecond / bidsPerAuction.
        // We generate bids at rate bidsPerSecond, each bid refers to
        // auctionId = seq / bidsPerAuction

        StreamStage<Object> auctions = pipeline
                .<Object>readFrom(eventSource(bidsPerSecond / bidsPerAuction, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = auctionMinDuration +
                                    getRandom(271 * seq, auctionMaxDuration - auctionMinDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0);

        StreamStage<Bid> bids = pipeline
                .readFrom(eventSource(bidsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> new Bid(seq, timestamp, seq / bidsPerAuction, 0)))
                .withNativeTimestamps(0);

        // NEXMark Query 4 start
        return auctions
                .merge(bids)
                .apply(joinAuctionToWinningBid(auctionMaxDuration))
                .map(t2 -> tuple3(t2.f0().category(), t2.f1().price(), t2.f0().expires())) // "catPriceExpires"
                .groupingKey(Tuple3::f0)
                .rollingAggregate(allOf(averagingLong(Tuple3::f1), lastSeen()))
                .map(catAndAggrResults -> {
                    int category = catAndAggrResults.getKey();
                    Tuple2<Double, Tuple3<Integer, Long, Long>> aggrResults = catAndAggrResults.getValue();
                    Tuple3<Integer, Long, Long> catPriceExpires = aggrResults.f1();
                    double averagePrice = aggrResults.f0();
                    long latestAuctionEnd = catPriceExpires.f2();
                    return tuple3(category, averagePrice, latestAuctionEnd);
                })
        // NEXMark Query 4 end

                .apply(stage -> determineLatency(stage, Tuple3::f2));
    }

    static <T> AggregateOperation1<T, MutableReference<T>, T> lastSeen() {
        return AggregateOperation
                .withCreate(MutableReference<T>::new)
                .andAccumulate(MutableReference<T>::set)
                .andExportFinish(MutableReference::get);
    }
}

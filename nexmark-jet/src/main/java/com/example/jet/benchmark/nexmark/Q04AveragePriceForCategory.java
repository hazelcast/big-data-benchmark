package com.example.jet.benchmark.nexmark;

import com.example.jet.benchmark.nexmark.model.Auction;
import com.example.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.example.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.function.ComparatorEx.comparingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class Q04AveragePriceForCategory extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int bidsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int auctionsPerSecond = min(1000, max(1000, bidsPerSecond));
        int bidsPerAuction = bidsPerSecond / auctionsPerSecond;

        // Mean duration of the auctions determines the number of keys stored
        // in the joining stage (joinAuctionToWinningBid). We randomize the
        // duration between 1/2 and 3/2 of the requested number so the average
        // comes out to 2.
        long auctionMinDuration = numDistinctKeys * 1000L / auctionsPerSecond / 2;
        long auctionMaxDuration = 3 * auctionMinDuration;
        System.out.format("Auction duration: %,d .. %,d ms%n", auctionMinDuration, auctionMaxDuration);

        // We generate auctions at rate bidsPerSecond / bidsPerAuction.
        // We generate bids at rate bidsPerSecond, each bid refers to
        // auctionId = seq / bidsPerAuction

        StreamStage<Object> auctions = pipeline
                .<Object>readFrom(eventSource("auctions", auctionsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = auctionMinDuration +
                                    getRandom(271 * seq, auctionMaxDuration - auctionMinDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0);

        StreamStage<Bid> bids = pipeline
                .readFrom(eventSource("bids", bidsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> new Bid(seq, timestamp, seq / bidsPerAuction, 0)))
                .withNativeTimestamps(0);

        // NEXMark Query 4 start
        StreamStage<Tuple3<Integer, Double, Long>> queryResult = auctions
                .merge(bids)
                .apply(JoinAuctionToWinningBidP.joinAuctionToWinningBid(auctionMaxDuration)) // Tuple2(auction, winningBid)
                .map(t -> tuple3(t.f0().category(), t.f1().price(), t.f0().expires())) // "catPriceExpires"
                .groupingKey(Tuple3::f0)
                .rollingAggregate(allOf(averagingLong(Tuple3::f1), maxBy(comparingLong(Tuple3::f2))))
                .map(catAndAggrResults -> {
                    int category = catAndAggrResults.getKey();
                    Tuple2<Double, Tuple3<Integer, Long, Long>> aggrResults = catAndAggrResults.getValue();
                    Tuple3<Integer, Long, Long> catPriceExpires = aggrResults.f1();
                    double averagePrice = aggrResults.f0();
                    long latestAuctionEnd = catPriceExpires.f2();
                    return tuple3(category, averagePrice, latestAuctionEnd);
                });
        // NEXMark Query 4 end

        // queryResult: Tuple3(category, averagePrice, latestAuctionEnd)
        return queryResult.apply(determineLatency(Tuple3::f2));
    }
}

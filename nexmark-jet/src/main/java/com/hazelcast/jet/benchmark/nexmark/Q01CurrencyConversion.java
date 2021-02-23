package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.eventSource;

public class Q01CurrencyConversion extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(Pipeline pipeline, Properties props) throws ValidationException {
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int sievingFactor = eventsPerSecond / 8192;
        var input = pipeline
                .readFrom(eventSource("bids",
                        eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) ->
                                new Bid(seq, timestamp, seq % numDistinctKeys, getRandom(seq, 100))))
                .withNativeTimestamps(0);

        // NEXMark Query 1 start
        StreamStage<Bid> queryResult = input
                .map(bid1 -> new Bid(bid1.id(), bid1.timestamp(), bid1.auctionId(), bid1.price() * 8 / 10));
        // NEXMark Query 1 end

        return queryResult
                .filter(bid -> bid.id() % sievingFactor == 0)
                .apply(determineLatency(Bid::timestamp));
    }
}

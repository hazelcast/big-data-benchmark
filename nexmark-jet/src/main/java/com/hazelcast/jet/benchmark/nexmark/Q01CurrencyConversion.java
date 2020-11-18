package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

public class Q01CurrencyConversion extends BenchmarkBase {

    Q01CurrencyConversion() {
        super("q01-currency-conversion");
    }

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            StreamStage<Bid> input, Properties props
    ) throws ValidationException {
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = eventsPerSecond / 8192;
        return input
                .map(bid1 -> new Bid(bid1.seq(), bid1.timestamp(), bid1.auctionId(), bid1.price() * 8 / 10))

                .filter(bid -> bid.seq() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, Bid::timestamp));
    }
}

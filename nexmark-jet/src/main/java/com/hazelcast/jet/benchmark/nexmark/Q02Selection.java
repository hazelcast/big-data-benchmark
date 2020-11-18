package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.hazelcast.jet.datamodel.Tuple3.tuple3;

public class Q02Selection extends BenchmarkBase {

    Q02Selection() {
        super("q02-selection");
    }

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            StreamStage<Bid> input, Properties props
    ) throws ValidationException {
        int auctionIdModulus = 128;
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = eventsPerSecond / 64;

        return input
                .filter(bid -> bid.auctionId() % auctionIdModulus == 0)
                .map(bid -> tuple3(bid.timestamp(), bid.auctionId(), bid.price()))

                .filter(t3 -> t3.f0() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, Tuple3<Long, Long, Long>::f0));
    }
}

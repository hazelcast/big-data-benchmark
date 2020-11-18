package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.List;
import java.util.Properties;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;

public class Q05HotItems extends BenchmarkBase {

    Q05HotItems() {
        super("q05-hot-items");
    }

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            StreamStage<Bid> input, Properties props
    ) throws ValidationException {
        int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
        long slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
        return input
                .window(sliding(windowSize, slideBy))
                .groupingKey(Bid::auctionId)
                .aggregate(counting())
                .window(tumbling(slideBy))
                .aggregate(topN(10, comparing(KeyedWindowResult::result)))
                .apply(stage -> determineLatency(stage, WindowResult<List<KeyedWindowResult<Long, Long>>>::end));
    }

}

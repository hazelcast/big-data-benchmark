package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;

public class Q05HotItems extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
        long slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
        return pipeline
                .readFrom(eventSource(eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) ->
                        new Bid(seq, timestamp, seq % numDistinctKeys, 0)))
                .withNativeTimestamps(0)
                .window(sliding(windowSize, slideBy))
                .groupingKey(Bid::auctionId)
                .aggregate(counting())
                .window(tumbling(slideBy))
                .aggregate(topN(10, comparing(KeyedWindowResult::result)))

                .apply(stage -> determineLatency(stage, WindowResult::end));
    }
}

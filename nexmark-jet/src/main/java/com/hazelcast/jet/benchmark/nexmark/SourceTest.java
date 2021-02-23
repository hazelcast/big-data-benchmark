package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

/**
 * Not a benchmark, just a tool to confirm the source generates
 * exactly as many events per second as configured.
 */
public class SourceTest extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
        long slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
        var input = pipeline
                .readFrom(eventSource("bids", eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> new Bid(seq, timestamp, seq % numDistinctKeys, getRandom(seq, 100))))
                .withNativeTimestamps(0);
        return input
                .window(sliding(windowSize, slideBy))
                .aggregate(counting())
                .peek()
                .apply(determineLatency(WindowResult::end));
    }
}

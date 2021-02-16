package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Auction;
import com.hazelcast.jet.benchmark.nexmark.model.Person;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Properties;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.pickAny;
import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.eventSource;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class Q08MonitorNewUsers extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
        long slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
        int sievingFactor = numDistinctKeys / 100;

        StreamStage<Person> persons = pipeline
                .readFrom(eventSource(eventsPerSecond / 2, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) -> {
                    long id = getRandom(seq, numDistinctKeys);
                    return new Person(id, timestamp, "Seller #" + id, null);
                }))
                .withNativeTimestamps(0);

        StreamStage<Auction> auctions = pipeline
                .readFrom(eventSource(eventsPerSecond / 2, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) -> {
                    long sellerId = getRandom(137 * seq, numDistinctKeys);
                    return new Auction(0, timestamp, sellerId, 0, 0);
                }))
                .withNativeTimestamps(0);

        return persons
                .window(sliding(windowSize, slideBy))
                .groupingKey(Person::id)
                .aggregate2(pickAny(), auctions.groupingKey(Auction::sellerId), counting())
                .filter(kwr -> kwr.result().f0() != null && kwr.result().f1() > 0)

                .filter(kwr -> kwr.key() % sievingFactor == 0)
                .apply(stage -> determineLatency(stage, WindowResult::end));
    }
}

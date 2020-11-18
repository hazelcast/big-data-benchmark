package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static java.util.stream.Collectors.toList;

public class Q13BoundedSideInput extends BenchmarkBase {

    Q13BoundedSideInput() {
        super("q13-bounded-side-input");
    }

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            StreamStage<Bid> input, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = eventsPerSecond / 8192;

        List<Entry<Long, String>> descriptionList = IntStream
                .range(0, numDistinctKeys)
                .mapToObj(i -> entry((long) i, String.format("Auctioned Item #%05d", i)))
                .collect(toList());

        BatchStage<Entry<Long, String>> itemDescriptions =
                input.getPipeline().readFrom(TestSources.items(descriptionList));

        return input.hashJoin(itemDescriptions, joinMapEntries(Bid::auctionId), Tuple2::tuple2)

                    .filter(t2 -> t2.f0().seq() % sievingFactor == 0)
                    .apply(stage -> determineLatency(stage, t2 -> t2.f0().timestamp()));
    }
}

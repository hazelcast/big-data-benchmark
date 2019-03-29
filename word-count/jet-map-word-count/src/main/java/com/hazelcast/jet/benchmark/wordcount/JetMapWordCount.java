package com.hazelcast.jet.benchmark.wordcount;


import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.server.JetBootstrap;

import java.util.StringTokenizer;

import static com.hazelcast.jet.Traversers.traverseEnumeration;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;

public class JetMapWordCount {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage <name>");
            return;
        }
        JetInstance client = JetBootstrap.getInstance();

        String sourceMap = args[0];
        String sinkMap = sourceMap + "-out";

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Object, String>map(sourceMap))
         .flatMap(entry -> traverseEnumeration(new StringTokenizer(entry.getValue())))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .drainTo(Sinks.map(sinkMap));

        JobConfig config = new JobConfig();
        config.addClass(JetMapWordCount.class);

        try {
            long start = System.currentTimeMillis();
            client.newJob(p, config).join();
            System.out.println("Time=" + (System.currentTimeMillis() - start));
        } finally {
            client.shutdown();
        }
    }
}

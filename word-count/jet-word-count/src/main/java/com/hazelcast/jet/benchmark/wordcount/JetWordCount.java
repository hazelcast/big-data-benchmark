package com.hazelcast.jet.benchmark.wordcount;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.hadoop.HdfsSinks;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.StringTokenizer;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class JetWordCount {

    public static void main(String[] args) throws Exception {
        JetInstance client = Jet.newJetClient();

        String inputPath = args[0];
        String outputPath = args[1] + "_" + System.currentTimeMillis();

        JobConf conf = new JobConf();
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(conf, new Path(inputPath));
        TextOutputFormat.setOutputPath(conf, new Path(outputPath));

        Pipeline p = Pipeline.create();
        p.drawFrom(HdfsSources.hdfs(conf, (k, v) -> v.toString()))
         .flatMap((String line) -> {
             StringTokenizer s = new StringTokenizer(line);
             return () -> s.hasMoreTokens() ? s.nextToken() : null;
         })
         .groupingKey(wholeItem())
         .aggregate(counting())
         .drainTo(HdfsSinks.hdfs(conf, entryKey(), entryValue()));

        JobConfig config = new JobConfig();
        config.addClass(JetWordCount.class);

        try {
            long start = System.currentTimeMillis();
            client.newJob(p, config).join();
            System.out.println("Time=" + (System.currentTimeMillis() - start));

        } finally {
            client.shutdown();
        }
    }
}

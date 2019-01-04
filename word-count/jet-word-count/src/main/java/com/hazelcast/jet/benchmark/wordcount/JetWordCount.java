package com.hazelcast.jet.benchmark.wordcount;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.hadoop.HdfsSinks;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.server.JetBootstrap;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.StringTokenizer;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class JetWordCount {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ");
            System.out.println("<hdfsUri> <inputPath> <outputPath>");
            return;
        }

        JetInstance client = JetBootstrap.getInstance();

        String hdfsUri = args[0];
        String inputPath = args[1];
        String outputPath = args[2] + "_" + System.currentTimeMillis();

        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
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

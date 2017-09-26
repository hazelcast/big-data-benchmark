package com.hazelcast.jet.benchmark.wordcount;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.StringTokenizer;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.HdfsProcessors.readHdfs;
import static com.hazelcast.jet.core.processor.HdfsProcessors.writeHdfs;
import static com.hazelcast.jet.core.processor.Processors.accumulateByKey;
import static com.hazelcast.jet.core.processor.Processors.combineByKey;
import static com.hazelcast.jet.core.processor.Processors.flatMap;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class JetWordCount {

    public static void main(String[] args) throws Exception {
        JetInstance client = Jet.newJetClient();

        String inputPath = args[0];
        String outputPath = args[1] + "_" + System.currentTimeMillis();

        DAG dag = new DAG();
        JobConf conf = new JobConf();
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(conf, new Path(inputPath));
        TextOutputFormat.setOutputPath(conf, new Path(outputPath));

        Vertex producer = dag.newVertex("reader", readHdfs(conf,
                (k, v) -> v.toString())).localParallelism(3);

        Vertex tokenizer = dag.newVertex("tokenizer",
                flatMap((String line) -> {
                    StringTokenizer s = new StringTokenizer(line);
                    return () -> s.hasMoreTokens() ? s.nextToken() : null;
                })
        );

        // word -> (word, count)
        Vertex accumulate = dag.newVertex("accumulate", accumulateByKey(wholeItem(), counting()));

        // (word, count) -> (word, count)
        Vertex combine = dag.newVertex("combine", combineByKey(counting()));
        Vertex consumer = dag.newVertex("writer", writeHdfs(conf)).localParallelism(1);

        dag.edge(between(producer, tokenizer))
           .edge(between(tokenizer, accumulate)
                   .partitioned(wholeItem(), HASH_CODE))
           .edge(between(accumulate, combine)
                   .distributed()
                   .partitioned(entryKey()))
           .edge(between(combine, consumer));

        JobConfig config = new JobConfig();
        config.addClass(JetWordCount.class);

        try {
            long start = System.currentTimeMillis();
            client.newJob(dag, config).execute().get();
            System.out.println("Time=" + (System.currentTimeMillis() - start));

        } finally {
            client.shutdown();
        }
    }
}

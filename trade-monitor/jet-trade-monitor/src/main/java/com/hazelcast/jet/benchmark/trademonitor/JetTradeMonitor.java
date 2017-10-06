package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.TimestampedEntry;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.server.JetBootstrap;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.core.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.lang.System.currentTimeMillis;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            System.err.println("Usage:");
            System.err.println("  " + JetTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs> <snapshotIntervalMs> <snapshotMode> <outputPath>");
            System.err.println();
            System.err.println("<snapshotMode> - \"exactly-once\" or \"at-least-once\"");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        String offsetReset = args[2];
        int lagMs = Integer.parseInt(args[3]);
        int windowSize = Integer.parseInt(args[4]);
        int slideBy = Integer.parseInt(args[5]);
        int snapshotInterval = Integer.parseInt(args[6]);
        ProcessingGuarantee guarantee = ProcessingGuarantee.valueOf(args[7].toUpperCase().replace('-', '_'));
        String outputPath = args[8];

        Properties kafkaProps = getKafkaProperties(brokerUri, offsetReset);

        WindowDefinition windowDef = slidingWindowDef(windowSize, slideBy);
        AggregateOperation1<Object, LongAccumulator, Long> counting = AggregateOperations.counting();

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(kafkaProps, topic))
                              .localParallelism(1);
        Vertex extractTrade = dag.newVertex("extract-trade", mapP(entryValue()));
        Vertex insertWm = dag.newVertex("insert-wm",
                insertWatermarksP(Trade::getTime, withFixedLag(lagMs), emitByFrame(windowDef)));
        Vertex accumulateByF = dag.newVertex("accumulate-by-frame",
                accumulateByFrameP(Trade::getTicker, Trade::getTime, TimestampKind.EVENT, windowDef, counting));
        Vertex slidingW = dag.newVertex("sliding-window", combineToSlidingWindowP(windowDef, counting));
        Vertex formatOutput = dag.newVertex("format-output",
                mapP((TimestampedEntry entry) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - entry.getTimestamp();
                    return Instant.ofEpochMilli(entry.getTimestamp()).atZone(ZoneId.systemDefault()).toLocalTime().toString()
                            + "," + entry.getKey()
                            + "," + entry.getValue()
                            + "," + timeMs
                            + "," + (latencyMs - lagMs);
                }));
        Vertex fileSink = dag.newVertex("write-file", writeFileP(outputPath))
                .localParallelism(1);

        dag
                .edge(between(readKafka, extractTrade).isolated())
                .edge(between(extractTrade, insertWm).isolated())
                .edge(between(insertWm, accumulateByF).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(accumulateByF, slidingW).partitioned(entryKey())
                                                 .distributed())
                .edge(between(slidingW, formatOutput).isolated())
                .edge(between(formatOutput, fileSink));


//        Jet.newJetInstance();

        JetInstance jet = JetBootstrap.getInstance();
        System.out.println("Executing job..");
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(snapshotInterval);
        config.setProcessingGuarantee(guarantee);
        Future<Void> future = jet.newJob(dag, config).getFuture();

        System.in.read();

        System.out.println("Cancelling job...");
        future.cancel(true);
        Thread.sleep(1000);
        jet.shutdown();
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        //props.setProperty("metadata.max.age.ms", "5000");
        return props;
    }
}

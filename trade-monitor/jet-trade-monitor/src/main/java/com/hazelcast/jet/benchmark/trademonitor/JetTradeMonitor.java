package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.AggregateOperations;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.PunctuationPolicies;
import com.hazelcast.jet.TimestampKind;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.server.JetBootstrap;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.processor.KafkaProcessors.streamKafka;
import static com.hazelcast.jet.processor.Processors.accumulateByFrame;
import static com.hazelcast.jet.processor.Processors.combineToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.insertPunctuation;
import static com.hazelcast.jet.processor.Processors.map;
import static com.hazelcast.jet.processor.Sinks.writeFile;
import static java.lang.System.currentTimeMillis;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            System.err.println("Usage:");
            System.err.println("  " + JetTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs> <outputPath>");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        String offsetReset = args[2];
        int lagMs = Integer.parseInt(args[3]);
        int windowSize = Integer.parseInt(args[4]);
        int slideBy = Integer.parseInt(args[5]);
        String outputPath = args[6];

        Properties kafkaProps = getKafkaProperties(brokerUri, offsetReset);

        WindowDefinition windowDef = slidingWindowDef(windowSize, slideBy);
        AggregateOperation<Object, LongAccumulator, Long> counting = AggregateOperations.counting();

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("read-kafka", streamKafka(kafkaProps, topic))
                .localParallelism(4);
        Vertex extractTrade = dag.newVertex("extract-trade", map(entryValue()));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertPunctuation(Trade::getTime, () -> PunctuationPolicies.withFixedLag(lagMs).throttleByFrame(windowDef)));
        Vertex accumulateByF = dag.newVertex("accumulate-by-frame",
                accumulateByFrame(Trade::getTicker, Trade::getTime, TimestampKind.EVENT, windowDef, counting));
        Vertex slidingW = dag.newVertex("sliding-window", combineToSlidingWindow(windowDef, counting));
        Vertex formatOutput = dag.newVertex("format-output",
                map((TimestampedEntry entry) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - entry.getTimestamp();
                    return String.format("%d,%s,%s,%d,%d", entry.getTimestamp(), entry.getKey(), entry.getValue(),
                            timeMs, latencyMs);
                }));
        Vertex fileSink = dag.newVertex("write-file", writeFile(outputPath));

        dag
                .edge(between(readKafka, extractTrade).oneToMany())
                .edge(between(extractTrade, insertPunctuation).oneToMany())
                .edge(between(insertPunctuation, accumulateByF).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(accumulateByF, slidingW).partitioned(entryKey())
                                                 .distributed())
                .edge(between(slidingW, formatOutput).oneToMany())
                .edge(between(formatOutput, fileSink));


//        Jet.newJetInstance();

        JetInstance jet = JetBootstrap.getInstance();
        System.out.println("Executing job..");
        Future<Void> execute = jet.newJob(dag).execute();

        System.in.read();

        System.out.println("Cancelling job...");
        execute.cancel(true);
        try {
            execute.get();
        } finally {
            jet.shutdown();
        }
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }
}

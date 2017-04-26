package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JobSubmitter;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.DistributedFunctions.entryValue;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.connector.kafka.StreamKafkaP.streamKafka;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndRetention;
import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowingProcessors.groupByFrame;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage:");
            System.err.println("  JetTradeMonitor <bootstrap.servers> <topic> <output_path>");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        String outputPath = args[2];

        DAG dag = new DAG();
        Properties kafkaProps = getKafkaProperties(brokerUri);
        Vertex readKafka = dag.newVertex("read-kafka", streamKafka(kafkaProps, topic));
        Vertex extractTrade = dag.newVertex("extract-event", map(entryValue()));
        WindowDefinition windowDef = WindowDefinition.slidingWindowDef(1000, 10);
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertPunctuation(Trade::getTime, () -> cappingEventSeqLagAndRetention(1, 100)
                        .throttleByFrame(windowDef)));
        Vertex groupByF = dag.newVertex("group-by-frame",
                groupByFrame(Trade::getTicker, Trade::getTime, windowDef, counting()));
        Vertex slidingW = dag.newVertex("sliding-window",
                slidingWindow(windowDef, counting()));
        Vertex addTimestamp = dag.newVertex("timestamp",
                map(f -> new TimestampedFrame((Frame) f, System.currentTimeMillis())));
        Vertex sink = dag.newVertex("sink", writeFile(Paths.get(outputPath, "output").toString()));

        dag
                .edge(between(readKafka, extractTrade).oneToMany())
                .edge(between(extractTrade, insertPunctuation).oneToMany())
                .edge(between(insertPunctuation, groupByF).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(groupByF, slidingW).partitioned(Frame<Object, Object>::getKey)
                                                 .distributed())
                .edge(between(slidingW, addTimestamp).oneToMany())
                .edge(between(addTimestamp, sink));

        JetInstance client = Jet.newJetClient();
        JobSubmitter.newJob(client, dag).execute().get();
    }

    private static Properties getKafkaProperties(String brokerUrl) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private static class TimestampedFrame<K, V> {

        private final Frame<K, V> frame;
        private final long timestamp;

        TimestampedFrame(Frame<K, V> frame, long timestamp) {
            this.frame = frame;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return frame.getSeq() + "," + frame.getKey() + "," + frame.getValue() + "," + timestamp;
        }
    }

}

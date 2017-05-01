package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.Accumulators.MutableLong;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JobSubmitter;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.jet.windowing.WindowOperation;
import com.hazelcast.jet.windowing.WindowOperations;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.DistributedFunctions.entryValue;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.connector.kafka.StreamKafkaP.streamKafka;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLag;
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.windowing.WindowingProcessors.groupByFrame;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;
import static java.lang.System.currentTimeMillis;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            System.err.println("Usage:");
            System.err.println("  " + JetTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <lag> <windowSizeMs> <slideByMs> <outputPath>");
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
        WindowOperation<Object, MutableLong, Long> counting = WindowOperations.counting();

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("readKafka", streamKafka(kafkaProps, topic));
        Vertex extractTrade = dag.newVertex("extractTrade", map(entryValue()));
        Vertex insertPunctuation = dag.newVertex("insertPunctuation",
                insertPunctuation(Trade::getTime, () -> cappingEventSeqLag(lagMs).throttleByFrame(windowDef)));
        Vertex groupByF = dag.newVertex("groupByF",
                groupByFrame(Trade::getTicker, Trade::getTime, windowDef, counting));
        Vertex slidingW = dag.newVertex("slidingW", slidingWindow(windowDef, counting));
        Vertex mapToLatency = dag.newVertex("formatFrame",
                map((Frame frame) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - frame.getSeq();
                    return String.format("%d,%s,%s,%d, %d", frame.getSeq(), frame.getKey(), frame.getValue(),
                            timeMs, latencyMs, latencyMs - lagMs);
                }));
        Vertex fileSink = dag.newVertex("fileSink", writeFile(outputPath));

        dag
                .edge(between(readKafka, extractTrade)
                        .oneToMany())
                .edge(between(extractTrade, insertPunctuation)
                        .oneToMany())
                .edge(between(insertPunctuation, groupByF)
                        .partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(groupByF, slidingW)
                        .partitioned(Frame<Object, Object>::getKey)
                        .distributed())
                .edge(between(slidingW, mapToLatency)
                        .oneToMany())
                .edge(between(mapToLatency, fileSink));

        JetInstance jetInstance = Jet.newJetInstance();
        jetInstance.newJob(dag).execute().get();

        JetInstance client = Jet.newJetClient();
        JobSubmitter.newJob(client, dag).execute().get();
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

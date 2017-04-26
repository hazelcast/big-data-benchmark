package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.jet.windowing.WindowOperation;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.DistributedFunctions.entryValue;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.connector.kafka.StreamKafkaP.streamKafka;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLag;
import static com.hazelcast.jet.windowing.WindowingProcessors.groupByFrame;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;

public class JetLatencyMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage:");
            System.err.println("  "+JetLatencyMonitor.class.getSimpleName()+" <bootstrap.servers> <topic> <slideByMs> <outputFile>");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        int slideBy = Integer.parseInt(args[2]);
        String outputFile = args[3];

        JetInstance jetInstance = Jet.newJetInstance();
        Jet.newJetInstance();

        Properties kafkaProps = getKafkaProperties(brokerUri);
        WindowDefinition windowDef = WindowDefinition.slidingWindowDef(10000, slideBy);
        WindowOperation<Trade, ?, Long> winOpAverageTradePrice = averageLongToLong(Trade::getPrice);

        int lag = 1000;

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("readKafka", streamKafka(kafkaProps, topic));
        Vertex extractTrade = dag.newVertex("extractTrade", map(entryValue()));
        Vertex insertPunctuation = dag.newVertex("insertPunctuation",
                insertPunctuation(Trade::getTime, () -> cappingEventSeqLag(lag)
                        .throttleByFrame(windowDef)));
        Vertex groupByF = dag.newVertex("groupByF",
                groupByFrame(Trade::getTicker, Trade::getTime, windowDef, winOpAverageTradePrice));
        Vertex slidingW = dag.newVertex("slidingW",
                slidingWindow(windowDef, winOpAverageTradePrice));
        Vertex mapToLatency = dag.newVertex("mapToLatency",
                map((Frame frame) -> new Frame<>(frame.getSeq(), 0, System.currentTimeMillis() - frame.getSeq() - lag)));
        Vertex fileSink = dag.newVertex("fileSink", writeFile(outputFile)).localParallelism(1);

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

        // peeks
//        Vertex peek = dag.newVertex("peek", PeekP::new);
//        dag.edge(from(extractTrade, 1).to(peek).oneToMany());
//        dag.edge(from(slidingW, 1).to(peek));

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("jet");
        clientConfig.getGroupConfig().setPassword("jet-pass");
        JetInstance client = Jet.newJetClient(clientConfig);
        client.newJob(dag).execute();
    }

    private static <T> WindowOperation<T, ?, Long> averageLongToLong(Distributed.ToLongFunction<T> mapper) {
        return WindowOperation.of(
                TupleLongLong::new,
                (acc, item) -> {
                    acc.sum = Math.addExact(acc.sum, mapper.applyAsLong(item));
                    acc.count++;
                },
                (acc1, acc2) -> {
                    acc1.sum = Math.addExact(acc1.sum, acc2.sum);
                    acc1.count = Math.addExact(acc1.count, acc2.count);
                    return acc1;
                },
                (acc1, acc2) -> {
                    acc1.sum = Math.subtractExact(acc1.sum, acc2.sum);
                    acc1.count = Math.subtractExact(acc1.count, acc2.count);
                    return acc1;
                },
                acc -> acc.count > 0 ? acc.sum / acc.count : Long.MIN_VALUE
        );
    }

    static class PeekP extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) throws Exception {
            getLogger().info(item.toString());
            return true;
        }

//        @Override
//        protected boolean tryProcessPunc(int ordinal, Punctuation punc) {
//            System.out.println(punc);
//            return true;
//        }
    }

    private static Properties getKafkaProperties(String brokerUrl) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    public static class TupleLongLong implements Serializable {
        public long sum;
        public long count;
    }
}

package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.InsertPunctuationP;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.DistributedFunctions.entryValue;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.connector.kafka.ReadKafkaP.readKafka;
import static com.hazelcast.jet.stream.DistributedCollectors.counting;
import static com.hazelcast.jet.windowing.FrameProcessors.groupByFrame;
import static com.hazelcast.jet.windowing.FrameProcessors.slidingWindow;
import static com.hazelcast.jet.windowing.PunctuationKeepers.cappingEventSeqLag;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];

        JetInstance jetInstance = Jet.newJetInstance();

        DAG dag = new DAG();
        Properties kafkaProps = getKafkaProperties(brokerUri);
        Vertex readKafka = dag.newVertex("read-kafka", readKafka(kafkaProps, topic))
                .localParallelism(8);
        Vertex extractTrade = dag.newVertex("extract-event", map(entryValue()));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                () -> new InsertPunctuationP<>(Trade::getTime, cappingEventSeqLag(2000),
                        500L, 500L));
        Vertex groupByF = dag.newVertex("group-by-frame",
                groupByFrame(Trade::getTicker, Trade::getTime, 1000, 0, counting())
        );
        Vertex slidingW = dag.newVertex("sliding-window",
                slidingWindow(1000, 1000, counting()));
        Vertex peek = dag.newVertex("peek", PeekP::new).localParallelism(1);
        Vertex sink = dag.newVertex("sink", Processors.writeMap("sink")).localParallelism(1);

        dag
           .edge(between(readKafka, extractTrade).oneToMany())
           .edge(between(extractTrade, insertPunctuation).oneToMany())
           .edge(between(insertPunctuation, groupByF).partitioned(Trade::getTicker, HASH_CODE))
           .edge(between(groupByF, slidingW).partitioned(Frame<Object, Object>::getKey)
                                                     .distributed())
           .edge(from(slidingW, 1).to(peek))
           .edge(between(slidingW, sink));

        JetInstance client = Jet.newJetClient();

        IMap<String, Long> sinkMap = client.getMap("sink");
        client.newJob(dag).execute();

        while (true) {
            System.out.println(sinkMap.entrySet().stream().mapToLong(Entry::getValue).sum());
            Thread.sleep(1000);
        }
    }

    static class PeekP extends AbstractProcessor {
        long start = 0;
        @Override
        protected boolean tryProcess(int ordinal, Object item) throws Exception {
            if (start == 0) {
                start = System.currentTimeMillis();
            }
            getLogger().info(System.currentTimeMillis() - start + "ms elapsed since first output");
            return true;
        }
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
}

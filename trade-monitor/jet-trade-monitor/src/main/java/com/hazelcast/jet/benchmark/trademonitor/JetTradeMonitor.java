package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.jet.windowing.WindowMaker;
import com.hazelcast.jet.windowing.WindowingProcessors;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.DistributedFunctions.entryValue;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.connector.kafka.ReadKafkaP.readKafka;
import static com.hazelcast.jet.stream.DistributedCollectors.counting;
import static com.hazelcast.jet.windowing.PunctuationKeepers.cappingEventSeqLag;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage:");
            System.err.println("  TradeProducer <bootstrap.servers> <topic>");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];

        JetInstance jetInstance = Jet.newJetInstance();

        DAG dag = new DAG();
        Properties kafkaProps = getKafkaProperties(brokerUri);
        Vertex readKafka = dag.newVertex("read-kafka", readKafka(kafkaProps, topic));
        Vertex extractTrade = dag.newVertex("extract-event", map(entryValue()));
        WindowDefinition tumblingWinOf100 = new WindowDefinition(100, 0, 1);
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertPunctuation(Trade::getTime, cappingEventSeqLag(1),
                        1L, 500L));
        Vertex groupByF = dag.newVertex("group-by-frame",
                WindowingProcessors.groupByFrame(Trade::getTicker, Trade::getTime, tumblingWinOf100, counting()));
        Vertex slidingW = dag.newVertex("sliding-window",
                slidingWindow(tumblingWinOf100, WindowMaker.fromCollector(counting())));
        Vertex filterPuncs = dag.newVertex("filterPuncs",
                Processors.filter(event -> !(event instanceof Punctuation)));
        Vertex addTimestamp = dag.newVertex("timestamp",
                Processors.map(f -> new TimestampedFrame((Frame) f, System.currentTimeMillis())));
        Vertex sink = dag.newVertex("sink", Processors.writeList("sink")).localParallelism(1);

        dag
           .edge(between(readKafka, extractTrade).oneToMany())
           .edge(between(extractTrade, insertPunctuation).oneToMany())
           .edge(between(insertPunctuation, groupByF).partitioned(Trade::getTicker, HASH_CODE))
           .edge(between(groupByF, slidingW).partitioned(Frame<Object, Object>::getKey)
                                                     .distributed())
           .edge(between(slidingW, filterPuncs).oneToMany())
           .edge(between(filterPuncs, addTimestamp).oneToMany())
           .edge(between(addTimestamp, sink));

        JetInstance client = Jet.newJetClient();

        IMap<String, Long> sinkMap = client.getMap("sink");
        client.newJob(dag).execute();

        while (true) {
            System.out.println(sinkMap.entrySet().stream().mapToLong(e -> e.getValue()).sum());
            Thread.sleep(1000);
        }
    }

    static class PeekP extends StreamingProcessorBase {

        private long start;

        @Override
        protected void init(Context context) throws Exception {
            start = System.currentTimeMillis();
            super.init(context);
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) throws Exception {
            Map.Entry e = (Map.Entry) item;
            if (e.getKey().equals("AAPL")) {
                getLogger().info("Window: " + item);
                getLogger().info("Elapsed: " + (System.currentTimeMillis() - start));
            }
            return true;
        }

        @Override
        protected boolean tryProcessPunc(int ordinal, Punctuation punc) {
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

    private static class TimestampedFrame<K, V> {

        private final Frame<K, V> frame;
        private final long timestamp;

        TimestampedFrame(Frame<K,V> frame, long timestamp) {
            this.frame = frame;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return frame.getSeq() + "," + frame.getKey() + "," + frame.getValue() + "," + timestamp;
        }
    }
}

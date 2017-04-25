package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.jet.windowing.WindowOperation;
import com.hazelcast.jet.windowing.WindowingProcessors;
import com.hazelcast.logging.ILogger;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.connector.kafka.StreamKafkaP.streamKafka;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndLull;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;

public class JetLatencyMonitor {

    private static final AtomicLong totalSum = new AtomicLong();
    private static final AtomicLong totalCount = new AtomicLong();

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage:");
            System.err.println("  "+JetLatencyMonitor.class.getSimpleName()+" <bootstrap.servers> <topic> <slideByMs> [<outputFile>]");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        int slideBy = Integer.parseInt(args[2]);
        String fileName = args.length > 3 ? args[3] : null;

        JetInstance jetInstance = Jet.newJetInstance();

        Properties kafkaProps = getKafkaProperties(brokerUri);
        WindowDefinition windowDef = WindowDefinition.slidingWindowDef(10000, slideBy);
        WindowOperation<Trade, TupleLongLong, Long> windowOperation = WindowOperation.of(
                TupleLongLong::new,
                (acc, trade) -> {
                    acc.sum = Math.addExact(acc.sum, trade.getIngestionTime());
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
                acc -> acc.sum / acc.count
        );

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("readKafka", streamKafka(kafkaProps, topic));
        Vertex extractTrade = dag.newVertex("extractTrade", map((Map.Entry<?, Trade> e) -> {
            e.getValue().setIngestionTime(System.nanoTime());
            return e.getValue();
        }));
        Vertex insertPunctuation = dag.newVertex("insertPunctuation",
                insertPunctuation(Trade::getTime, () -> cappingEventSeqLagAndLull(1000, 2000)
                        .throttleByFrame(windowDef)));
        Vertex groupByF = dag.newVertex("groupByF",
                WindowingProcessors.groupByFrame(Trade::getTicker, Trade::getTime, windowDef, windowOperation));
        Vertex slidingW = dag.newVertex("slidingW",
                slidingWindow(windowDef, windowOperation, false));
        Vertex sink1 = dag.newVertex("sink1",
                () -> new Processor() {
                    public ILogger logger;

                    @Override
                    public void init(Outbox outbox, Context context) {
                        logger = context.logger();
                    }

                    @Override
                    public void process(int ordinal, Inbox inbox) {
                        long now = System.nanoTime();
                        long localSum = 0, localCount = 0;
                        for (Object o; (o = inbox.poll()) != null; ) {
                            if (o instanceof Punctuation) {
                                continue;
                            }
                            Frame<String, Long> frame = (Frame<String, Long>) o;
//                            logger.info("sink1-frame=" + frame + ", now=" + System.nanoTime());
                            // frame contains the trade with maximum ingestionTime
                            long latency = now - frame.getValue();
                            localSum += latency;
                            localCount++;
                        };
                        //logger.info("sink1-frame drained=" + drainedCount + " frames, now=" + System.nanoTime() + ", iterationCnt=" + iterationCnt);

                        totalSum.addAndGet(localSum);
                        totalCount.addAndGet(localCount);
                    }
                }).localParallelism(1);

        dag
                .edge(between(readKafka, extractTrade).oneToMany())
                .edge(between(extractTrade, insertPunctuation).oneToMany())
                .edge(between(insertPunctuation, groupByF).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(groupByF, slidingW).partitioned(Frame<Object, Object>::getKey)
                                                 .distributed())
                .edge(between(slidingW, sink1));

        // add the file sink, if requested
        if (fileName != null) {
            Vertex sink2 = dag.newVertex("sink2", writeFile(fileName)).localParallelism(1);
            dag.edge(from(slidingW, 1).to(sink2));
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("jet");
        clientConfig.getGroupConfig().setPassword("jet-pass");
        JetInstance client = Jet.newJetClient(clientConfig);
        client.newJob(dag).execute();

        while (true) {
            Thread.sleep(1000);
            long sum = totalSum.get();
            long count = totalCount.get();
            totalSum.set(0);
            totalCount.set(0);
            System.out.println("average latency=" + (count != 0 ? sum / count / 1_000_000 + "ms" : "?") + ", count=" + count);
        }
    }

    static class PeekP extends AbstractProcessor {

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
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    public static class TupleLongLong {
        public long sum;
        public long count;
    }
}

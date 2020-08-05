package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.benchmark.trademonitor.KafkaTradeProducer.MessageType;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.benchmark.trademonitor.KafkaTradeProducer.MessageType.BYTE;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JetTradeMonitor {
    public static final String KAFKA_TOPIC = "trades";
    public static final long LATENCY_REPORTING_THRESHOLD_MS = 10;
    public static final long WARMUP_REPORTING_INTERVAL_MS = SECONDS.toMillis(2);
    public static final long MEASUREMENT_REPORTING_INTERVAL_MS = SECONDS.toMillis(10);
    public static final long BENCHMARKING_DONE_REPORT_INTERVAL_MS = SECONDS.toMillis(1);
    private static final String BENCHMARK_DONE_MESSAGE = "benchmarking is done";

    public static void main(String[] args) {
        System.out.println("Supplied arguments: " + Arrays.toString(args));
        if (args.length != 12) {
            System.err.println("Reads trade events from a Kafka topic named 'trades', performs sliding");
            System.err.println("window aggregation on them and records the pipeline's latency:");
            System.err.println("how much after the window's end timestamp was Jet able to emit the first");
            System.err.println("key-value pair of the window result.");
            System.err.println("Usage:");
            System.err.println("  " + JetTradeMonitor.class.getSimpleName() +
                    " <Kafka broker URI> <message offset auto-reset> <message type> \\");
            System.err.println("    <Kafka source local parallelism> <allowed event lag ms> \\");
            System.err.println("    <window size ms> <sliding step ms> \\");
            System.err.println("    <processing guarantee> <snapshot interval ms> \\");
            System.err.println("    <warmup seconds> <measurement seconds> <output path>");
            System.err.println();
            System.err.println("Processing guarantee is one of \"none\", \"exactly-once\", \"at-least-once\"");
            System.err.println("Message type is one of \"byte\", \"object\"");
            System.err.println("You can use underscore in the numbers, for example 1_000_000.");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j2");
        Logger logger = Logger.getLogger(JetTradeMonitor.class);
        String brokerUri = args[0];
        String offsetReset = args[1];
        MessageType messageType = MessageType.valueOf(args[2].toUpperCase());
        int kafkaSourceParallelism = parseIntArg(args[3]);
        int lagMs = parseIntArg(args[4]);
        int windowSize = parseIntArg(args[5]);
        int slideBy = parseIntArg(args[6]);
        ProcessingGuarantee guarantee = ProcessingGuarantee.valueOf(args[7].toUpperCase().replace('-', '_'));
        int snapshotInterval = parseIntArg(args[8]);
        int warmupSeconds = parseIntArg(args[9]);
        int measurementSeconds = parseIntArg(args[10]);
        String outputPath = args[11];
        logger.info(String.format("" +
                        "Starting Jet Trade Monitor with the following parameters:%n" +
                        "Kafka broker URI                  %s%n" +
                        "Message offset auto-reset         %s%n" +
                        "Message type                      %s%n" +
                        "Local parallelism of Kafka source %,d%n" +
                        "Allowed event lag                 %,d ms%n" +
                        "Window size                       %,d ms%n" +
                        "Window sliding step               %,d ms%n" +
                        "Processing guarantee              %s%n" +
                        "Snapshot interval                 %,d ms%n" +
                        "Warmup period                     %,d seconds%n" +
                        "Measurement period                %,d seconds%n" +
                        "Output path                       %s%n",
                brokerUri,
                offsetReset,
                messageType,
                kafkaSourceParallelism,
                lagMs,
                windowSize,
                slideBy,
                guarantee,
                snapshotInterval,
                warmupSeconds,
                measurementSeconds,
                outputPath
        ));
        Properties kafkaProps = getKafkaProperties(brokerUri, offsetReset, messageType);

        Pipeline pipeline = Pipeline.create();
        StreamStage<Trade> sourceStage;
        if (messageType == BYTE) {
            sourceStage = pipeline
                    .readFrom(KafkaSources.kafka(kafkaProps,
                            (ConsumerRecord<Object, byte[]> record) -> record.value(),
                            KAFKA_TOPIC))
                    .withoutTimestamps()
                    .mapUsingService(sharedService(ctx -> new Deserializer()), Deserializer::deserialize)
                    .addTimestamps(Trade::getTime, lagMs)
                    .setLocalParallelism(kafkaSourceParallelism);
        } else {
            sourceStage = pipeline
                    .readFrom(KafkaSources.kafka(kafkaProps,
                            (ConsumerRecord<Object, Trade> record) -> record.value(),
                            KAFKA_TOPIC))
                    .withTimestamps(Trade::getTime, lagMs)
                    .setLocalParallelism(kafkaSourceParallelism);
        }
        StreamStage<Tuple2<Long, Long>> latencies = sourceStage
                .groupingKey(Trade::getTicker)
                .window(sliding(windowSize, slideBy))
                .aggregate(counting())
                .mapStateful(DetermineLatency::new, DetermineLatency::map);
        long warmupTimeMillis = SECONDS.toMillis(warmupSeconds);
        long totalTimeMillis = SECONDS.toMillis(warmupSeconds + measurementSeconds);
        latencies
                .filter(t2 -> t2.f0() < totalTimeMillis)
                .map(t2 -> String.format("%d,%d", t2.f0(), t2.f1()))
                .writeTo(Sinks.files(new File(outputPath, "latency-log").getPath()));
        latencies
                .mapStateful(
                        () -> new RecordLatencyHistogram(warmupTimeMillis, totalTimeMillis),
                        RecordLatencyHistogram::map)
                .writeTo(Sinks.files(new File(outputPath, "latency-profile").getPath()));

        JobConfig config = new JobConfig();
        config.setName("JetTradeMonitor");
        config.setSnapshotIntervalMillis(snapshotInterval);
        config.setProcessingGuarantee(guarantee);

        JetInstance jet = Jet.bootstrappedInstance();
        Job job = jet.newJob(pipeline, config);
        Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
        System.out.println("Benchmarking job is now in progress, let it run until you see the message");
        System.out.println("\"" + BENCHMARK_DONE_MESSAGE + "\" in the Jet server log,");
        System.out.println("and then stop it here with Ctrl-C. The result files are on the server.");
        job.join();
    }

    private static int parseIntArg(String arg) {
        return Integer.parseInt(arg.replace("_", ""));
    }

    private static class DetermineLatency implements Serializable {
        private long startTimestamp;
        private long lastTimestamp;

        Tuple2<Long, Long> map(KeyedWindowResult<String, Long> kwr) {
            long timestamp = kwr.end();
            if (timestamp <= lastTimestamp) {
                return null;
            }
            if (lastTimestamp == 0) {
                startTimestamp = timestamp;
            }
            lastTimestamp = timestamp;

            long latency = System.currentTimeMillis() - timestamp;
            long count = kwr.result();
            if (latency == -1) { // very low latencies may be reported as negative due to clock skew
                latency = 0;
            }
            if (latency < 0) {
                throw new RuntimeException("Negative latency: " + latency);
            }
            if (latency >= LATENCY_REPORTING_THRESHOLD_MS) {
                System.out.format("Latency %,d ms (first seen key: %s, count %,d)%n", latency, kwr.getKey(), count);
            }
            return tuple2(timestamp - startTimestamp, latency);
        }
    }

    private static class RecordLatencyHistogram implements Serializable {
        private final long warmupTimeMillis;
        private final long totalTimeMillis;
        private Histogram histogram = new Histogram(5);

        public RecordLatencyHistogram(long warmupTimeMillis, long totalTimeMillis) {
            this.warmupTimeMillis = warmupTimeMillis;
            this.totalTimeMillis = totalTimeMillis;
        }

        String map(Tuple2<Long, Long> timestampAndLatency) {
            long timestamp = timestampAndLatency.f0();
            String timeMsg = String.format("%,d ", totalTimeMillis - timestamp);
            if (histogram == null) {
                if (timestamp % BENCHMARKING_DONE_REPORT_INTERVAL_MS == 0) {
                    System.out.format(BENCHMARK_DONE_MESSAGE + " -- %s%n", timeMsg);
                }
                return null;
            }
            if (timestamp < warmupTimeMillis) {
                if (timestamp % WARMUP_REPORTING_INTERVAL_MS == 0) {
                    System.out.format("warming up -- %s%n", timeMsg);
                }
            } else {
                if (timestamp % MEASUREMENT_REPORTING_INTERVAL_MS == 0) {
                    System.out.println(timeMsg);
                }
                histogram.recordValue(timestampAndLatency.f1());
            }
            if (timestamp >= totalTimeMillis) {
                try {
                    return exportHistogram(histogram);
                } finally {
                    histogram = null;
                }
            }
            return null;
        }

        private static String exportHistogram(Histogram histogram) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(bos);
            histogram.outputPercentileDistribution(out, 1.0);
            out.close();
            return bos.toString();
        }
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset, MessageType messageType) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer",
                (messageType == BYTE ? ByteArrayDeserializer.class : TradeDeserializer.class).getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        //props.setProperty("metadata.max.age.ms", "5000");
        return props;
    }

    private static class Deserializer {
        final ResettableByteArrayInputStream is = new ResettableByteArrayInputStream();
        final DataInputStream ois = new DataInputStream(is);

        Trade deserialize(byte[] bytes) {
            is.setBuffer(bytes);
            try {
                String ticker = ois.readUTF();
                long time = ois.readLong();
                int price = ois.readInt();
                int quantity = ois.readInt();
                return new Trade(time, ticker, quantity, price);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ResettableByteArrayInputStream extends ByteArrayInputStream {

        ResettableByteArrayInputStream() {
            super(new byte[0]);
        }

        void setBuffer(byte[] data) {
            super.buf = data;
            super.pos = 0;
            super.mark = 0;
            super.count = data.length;
        }
    }
}

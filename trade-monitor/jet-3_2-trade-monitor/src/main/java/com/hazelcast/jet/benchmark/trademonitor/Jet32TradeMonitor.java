package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.benchmark.Trade;
import com.hazelcast.jet.benchmark.Util;
import com.hazelcast.jet.benchmark.ValidationException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.benchmark.Util.KAFKA_TOPIC;
import static com.hazelcast.jet.benchmark.Util.ensureProp;
import static com.hazelcast.jet.benchmark.Util.loadProps;
import static com.hazelcast.jet.benchmark.Util.parseIntProp;
import static com.hazelcast.jet.benchmark.Util.props;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Jet32TradeMonitor {
    public static final String DEFAULT_PROPERTIES_FILENAME = "jet-trade-monitor.properties";
    public static final String PROP_BROKER_URI = "broker-uri";
    public static final String PROP_OFFSET_RESET = "offset-reset";
    public static final String PROP_KAFKA_SOURCE_LOCAL_PARALLELISM = "kafka-source-local-parallelism";
    public static final String PROP_WINDOW_SIZE_MILLIS = "window-size-millis";
    public static final String PROP_SLIDING_STEP_MILLIS = "sliding-step-millis";
    public static final String PROP_PROCESSING_GUARANTEE = "processing-guarantee";
    public static final String PROP_SNAPSHOT_INTERVAL_MILLIS = "snapshot-interval-millis";
    public static final String PROP_WARMUP_SECONDS = "warmup-seconds";
    public static final String PROP_MEASUREMENT_SECONDS = "measurement-seconds";
    public static final String PROP_OUTPUT_PATH = "output-path";

    public static final long LATENCY_REPORTING_THRESHOLD_MS = 10;
    public static final long WARMUP_REPORTING_INTERVAL_MS = SECONDS.toMillis(2);
    public static final long MEASUREMENT_REPORTING_INTERVAL_MS = SECONDS.toMillis(10);
    public static final long BENCHMARKING_DONE_REPORT_INTERVAL_MS = SECONDS.toMillis(1);
    public static final String BENCHMARK_DONE_MESSAGE = "benchmarking is done";

    public static void main(String[] args) {
        String propsPath = args.length > 0 ? args[0] : DEFAULT_PROPERTIES_FILENAME;
        Properties props = loadProps(propsPath);
        try {
            String brokerUri = ensureProp(props, PROP_BROKER_URI);
            String offsetReset = ensureProp(props, PROP_OFFSET_RESET);
            int kafkaSourceParallelism = parseIntProp(props, PROP_KAFKA_SOURCE_LOCAL_PARALLELISM);
            int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
            int slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
            String pgString = ensureProp(props, PROP_PROCESSING_GUARANTEE);
            ProcessingGuarantee guarantee = ProcessingGuarantee.valueOf(pgString.toUpperCase().replace('-', '_'));
            int snapshotInterval = parseIntProp(props, PROP_SNAPSHOT_INTERVAL_MILLIS);
            int warmupSeconds = parseIntProp(props, PROP_WARMUP_SECONDS);
            int measurementSeconds = parseIntProp(props, PROP_MEASUREMENT_SECONDS);
            String outputPath = ensureProp(props, PROP_OUTPUT_PATH);
            System.out.format(
                    "Starting Jet Trade Monitor with the following parameters:%n" +
                    "Kafka broker URI                  %s%n" +
                    "Message offset auto-reset         %s%n" +
                    "Local parallelism of Kafka source %,d%n" +
                    "Window size                       %,d ms%n" +
                    "Window sliding step               %,d ms%n" +
                    "Processing guarantee              %s%n" +
                    "Snapshot interval                 %,d ms%n" +
                    "Warmup period                     %,d seconds%n" +
                    "Measurement period                %,d seconds%n" +
                    "Output path                       %s%n",
                            brokerUri,
                            offsetReset,
                            kafkaSourceParallelism,
                            windowSize,
                            slideBy,
                            guarantee,
                            snapshotInterval,
                            warmupSeconds,
                            measurementSeconds,
                            outputPath
            );
            Properties kafkaProps = createKafkaProperties(brokerUri, offsetReset);

            Pipeline pipeline = Pipeline.create();
            StreamStage<Tuple2<Long, Long>> latencies = pipeline
                    .drawFrom(KafkaSources.kafka(kafkaProps,
                            (FunctionEx<ConsumerRecord<Object, Trade>, Trade>) ConsumerRecord::value,
                            KAFKA_TOPIC))
                    .withNativeTimestamps(0)
                    .setLocalParallelism(kafkaSourceParallelism)
                    .groupingKey(Trade::getTicker)
                    .window(sliding(windowSize, slideBy))
                    .aggregate(counting())
                    .mapStateful(DetermineLatency::new, DetermineLatency::map);
            long warmupTimeMillis = SECONDS.toMillis(warmupSeconds);
            long totalTimeMillis = SECONDS.toMillis(warmupSeconds + measurementSeconds);
            latencies
                    .filter(t2 -> t2.f0() < totalTimeMillis)
                    .map(t2 -> String.format("%d,%d", t2.f0(), t2.f1()))
                    .drainTo(Sinks.files(new File(outputPath, "latency-log").getPath()));
            latencies
                    .mapStateful(
                            () -> new RecordLatencyHistogram(warmupTimeMillis, totalTimeMillis),
                            RecordLatencyHistogram::map)
                    .drainTo(Sinks.files(new File(outputPath, "latency-profile").getPath()));

            JobConfig jobCfg = new JobConfig();
            jobCfg.setName("Trade Monitor Benchmark");
            jobCfg.setSnapshotIntervalMillis(snapshotInterval);
            jobCfg.setProcessingGuarantee(guarantee);

            JetInstance jet = JetBootstrap.getInstance();
            Job job = jet.newJob(pipeline, jobCfg);
            Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
            System.out.println("Benchmarking job is now in progress, let it run until you see the message");
            System.out.println("\"" + BENCHMARK_DONE_MESSAGE + "\" in the Jet server log,");
            System.out.println("and then stop it here with Ctrl-C. The result files are on the server.");
            job.join();
        } catch (ValidationException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println("Reads trade events from a Kafka topic named \"" + KAFKA_TOPIC + "\", performs sliding");
            System.err.println("window aggregation on them and records the pipeline's latency:");
            System.err.println("how much after the window's end timestamp was Jet able to emit the first");
            System.err.println("key-value pair of the window result.");
            System.err.println("Usage:");
            System.err.println("    " + Jet32TradeMonitor.class.getSimpleName() + " [props-file]");
            System.err.println();
            System.err.println(
                    "The default properties file is " + DEFAULT_PROPERTIES_FILENAME + " in the current directory.");
            System.err.println();
            System.err.println("An example of the required properties:");
            System.err.println(PROP_BROKER_URI + "=localhost:9092");
            System.err.println("# earliest or latest:");
            System.err.println(PROP_OFFSET_RESET + "=latest");
            System.err.println(PROP_KAFKA_SOURCE_LOCAL_PARALLELISM + "=4");
            System.err.println(PROP_WINDOW_SIZE_MILLIS + "=1_000");
            System.err.println(PROP_SLIDING_STEP_MILLIS + "=10");
            System.err.println("# none, at-least-once or exactly-once:");
            System.err.println(PROP_PROCESSING_GUARANTEE + "=at-least-once");
            System.err.println(PROP_SNAPSHOT_INTERVAL_MILLIS + "=10_000");
            System.err.println(PROP_WARMUP_SECONDS + "=40");
            System.err.println(PROP_MEASUREMENT_SECONDS + "=240");
            System.err.println(PROP_OUTPUT_PATH + "=benchmark-results");
            System.exit(1);
        }
    }

    private static Properties createKafkaProperties(String brokerUrl, String offsetReset) {
        return props(
                "bootstrap.servers", brokerUrl,
                "group.id", UUID.randomUUID().toString(),
                "key.deserializer", IntegerDeserializer.class.getName(),
                "value.deserializer", KafkaTradeDeserializer.class.getName(),
                "auto.offset.reset", offsetReset,
                "max.poll.records", "32768"
        );
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

    private static class TradeStreamSerializer implements StreamSerializer<Trade> {

        @Override
        public void write(ObjectDataOutput out, Trade trade) throws IOException {
            out.write(Util.serializeTrade(trade));
        }

        @Override
        public Trade read(ObjectDataInput in) throws IOException {
            byte[] blob = new byte[Util.TRADE_BLOB_SIZE];
            in.readFully(blob);
            return Util.deserializeTrade(blob);
        }

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void destroy() {
        }
    }
}

package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.benchmark.Trade;
import com.hazelcast.jet.benchmark.Util;
import com.hazelcast.jet.benchmark.ValidationException;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.hazelcast.jet.benchmark.Util.ensureProp;
import static com.hazelcast.jet.benchmark.Util.loadProps;
import static com.hazelcast.jet.benchmark.Util.parseIntProp;
import static com.hazelcast.jet.benchmark.Util.props;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class KafkaConsumerBenchmark {
    public static final String DEFAULT_PROPERTIES_FILENAME = "kafka-consumer-benchmark.properties";

    public static final String PROP_KAFKA_BROKER_URI = "kafka-broker-uri";
    public static final String PROP_PARTITION_COUNT = "partition-count";
    public static final String PROP_WARMUP_SECONDS = "warmup-seconds";
    public static final String PROP_MEASUREMENT_SECONDS = "measurement-seconds";
    public static final String PROP_LATENCY_REPORTING_THRESHOLD_MILLIS = "latency-reporting-threshold-millis";

    private static final Duration ONE_SECOND = Duration.of(1, ChronoUnit.SECONDS);

    public static void main(String[] args) {
        String propsPath = args.length > 0 ? args[0] : DEFAULT_PROPERTIES_FILENAME;
        Properties props = loadProps(propsPath);
        try {
            String brokerUri = ensureProp(props, PROP_KAFKA_BROKER_URI);
            int warmupSeconds = parseIntProp(props, PROP_WARMUP_SECONDS);
            int measurementSeconds = parseIntProp(props, PROP_MEASUREMENT_SECONDS);
            int latencyReportingThresholdMs = parseIntProp(props, PROP_LATENCY_REPORTING_THRESHOLD_MILLIS);
            int partitionCount = parseIntProp(props, PROP_PARTITION_COUNT);

            KafkaConsumer<Long, Trade> consumer = new KafkaConsumer<>(props(
                    "bootstrap.servers", brokerUri,
                    "group.id", UUID.randomUUID().toString(),
                    "key.deserializer", IntegerDeserializer.class.getName(),
                    "value.deserializer", KafkaTradeDeserializer.class.getName(),
                    "auto.offset.reset", "latest",
                    "max.poll.records", "32768"));
            consumer.assign(IntStream.range(0, partitionCount)
                                     .mapToObj(p -> new TopicPartition(Util.KAFKA_TOPIC, p))
                                     .collect(toList()));
            Histogram histogram = new Histogram(5);
            long benchmarkStart = System.currentTimeMillis();
            long measurementStart = benchmarkStart + SECONDS.toMillis(warmupSeconds);
            long benchmarkEnd = measurementStart + SECONDS.toMillis(measurementSeconds);
            long lastReport = benchmarkStart;
            while (true) {
                ConsumerRecords<Long, Trade> records = consumer.poll(ONE_SECOND);
                long now = System.currentTimeMillis();
                if (now >= benchmarkEnd) {
                    break;
                }
                if (now - lastReport >= SECONDS.toMillis(5)) {
                    if (now < measurementStart) {
                        System.out.format("Warmup %,d/%,d seconds%n",
                                MILLISECONDS.toSeconds(now - benchmarkStart),
                                (long) warmupSeconds);
                    } else {
                        System.out.format("Measurement %,d/%,d seconds%n",
                                MILLISECONDS.toSeconds(now - measurementStart),
                                (long) measurementSeconds);
                    }
                    lastReport = now;
                }
                if (records.isEmpty()) {
                    continue;
                }
                long batchLatency = 0;
                for (ConsumerRecord<Long, Trade> record : records) {
                    long latency = now - record.value().getTime();
                    batchLatency = max(latency, batchLatency);
                    if (now >= measurementStart) {
                        histogram.recordValue(latency);
                    }
                }
                if (batchLatency > latencyReportingThresholdMs) {
                    System.out.format("Latency %,d ms%n", batchLatency);
                }
            }
            histogram.outputPercentileDistribution(System.out, 1.0);
        } catch (ValidationException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println("Usage:");
            System.err.println("    " + KafkaConsumerBenchmark.class.getSimpleName() + " [props-file]");
            System.err.println();
            System.err.println(
                    "The default properties file is " + DEFAULT_PROPERTIES_FILENAME + " in the current directory.");
            System.err.println();
            System.err.println("An example of the required properties:");
            System.err.println(PROP_KAFKA_BROKER_URI + "=localhost:9092");
            System.err.println(PROP_WARMUP_SECONDS + "=10");
            System.err.println(PROP_MEASUREMENT_SECONDS + "=50");
            System.err.println(PROP_LATENCY_REPORTING_THRESHOLD_MILLIS + "=20");
            System.exit(1);
        }
    }
}

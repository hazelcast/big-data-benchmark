/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.benchmark.Trade;
import com.hazelcast.jet.benchmark.Util;
import com.hazelcast.jet.benchmark.ValidationException;
import org.HdrHistogram.Histogram;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.jet.benchmark.Util.KAFKA_TOPIC;
import static com.hazelcast.jet.benchmark.Util.ensureProp;
import static com.hazelcast.jet.benchmark.Util.loadProps;
import static com.hazelcast.jet.benchmark.Util.parseBooleanProp;
import static com.hazelcast.jet.benchmark.Util.parseIntProp;
import static com.hazelcast.jet.benchmark.Util.props;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FlinkTradeMonitor {
    public static final String DEFAULT_PROPERTIES_FILENAME = "flink-trade-monitor.properties";
    public static final String PROP_BROKER_URI = "broker-uri";
    public static final String PROP_OFFSET_RESET = "offset-reset";
    public static final String PROP_KAFKA_SOURCE_PARALLELISM = "kafka-source-parallelism";
    public static final String PROP_AGGREGATION_PARALLELISM = "aggregation-parallelism";
    public static final String PROP_WINDOW_SIZE_MILLIS = "window-size-millis";
    public static final String PROP_SLIDING_STEP_MILLIS = "sliding-step-millis";
    public static final String PROP_CHECKPOINT_DATA_URI = "checkpoint-data-uri";
    public static final String PROP_CHECKPOINT_INTERVAL_MILLIS = "checkpoint-interval-millis";
    public static final String PROP_ASYNCHRONOUS_SNAPSHOTS = "asynchronous-snapshots";
    public static final String PROP_STATE_BACKEND = "state-backend";
    public static final String STATE_BACKEND_FS = "fs";
    public static final String STATE_BACKEND_ROCKSDB = "rocksdb";
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
        StreamExecutionEnvironment env;
        try {
            String brokerUri = ensureProp(props, PROP_BROKER_URI);
            String offsetReset = ensureProp(props, PROP_OFFSET_RESET);
            int kafkaSourceParallelism = parseIntProp(props, PROP_KAFKA_SOURCE_PARALLELISM);
            int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
            int slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
            int aggregationParallelism = parseIntProp(props, PROP_AGGREGATION_PARALLELISM);
            int checkpointInterval = parseIntProp(props, PROP_CHECKPOINT_INTERVAL_MILLIS);
            String checkpointDataUri = ensureProp(props, PROP_CHECKPOINT_DATA_URI);
            boolean asyncSnapshots = parseBooleanProp(props, PROP_ASYNCHRONOUS_SNAPSHOTS);
            String stateBackendProp = ensureProp(props, PROP_STATE_BACKEND);
            int warmupSeconds = parseIntProp(props, PROP_WARMUP_SECONDS);
            int measurementSeconds = parseIntProp(props, PROP_MEASUREMENT_SECONDS);
            String outputPath = ensureProp(props, PROP_OUTPUT_PATH);
            System.out.format(
                    "Starting Flink Trade Monitor with the following parameters:%n" +
                    "Kafka broker URI            %s%n" +
                    "Message offset auto-reset   %s%n" +
                    "Parallelism of Kafka source %,d%n" +
                    "Window size                 %,d ms%n" +
                    "Window sliding step         %,d ms%n" +
                    "Parallelism of aggregation  %,d%n" +
                    "Checkpointing interval      %,d ms%n" +
                    "Checkpoint URI              %s%n" +
                    "Asynchronous snapshots?     %b%n" +
                    "State backend               %s%n" +
                    "Warmup period               %,d seconds%n" +
                    "Measurement period          %,d seconds%n" +
                    "Output path                 %s%n",
                            brokerUri,
                            offsetReset,
                            kafkaSourceParallelism,
                            windowSize,
                            slideBy,
                            aggregationParallelism,
                            checkpointInterval,
                            checkpointDataUri,
                            asyncSnapshots,
                            stateBackendProp,
                            warmupSeconds,
                            measurementSeconds,
                            outputPath
            );
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            if (checkpointInterval > 0) {
                env.enableCheckpointing(checkpointInterval);
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInterval);
            }
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 5000));
            StateBackend stateBackend;
            if (STATE_BACKEND_FS.equalsIgnoreCase(stateBackendProp)) {
                stateBackend = new FsStateBackend(checkpointDataUri, asyncSnapshots);
            } else if (STATE_BACKEND_ROCKSDB.equalsIgnoreCase(stateBackendProp)) {
                stateBackend = new RocksDBStateBackend(checkpointDataUri);
            } else {
                System.err.println("state-backend must be either \"" + STATE_BACKEND_FS + "\" or \"" +
                        STATE_BACKEND_ROCKSDB + "\", but is \"" + stateBackendProp + "\"");
                System.exit(1);
                return;
            }
            env.setStateBackend(stateBackend);

            SingleOutputStreamOperator<Tuple2<Long, Long>> latencies = env
                    .addSource(new FlinkKafkaConsumer<>(KAFKA_TOPIC, tradeDeserializationSchema(),
                            createKafkaProperties(brokerUri, offsetReset)))
                    .setParallelism(kafkaSourceParallelism)
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Trade>forMonotonousTimestamps()
                            .withTimestampAssigner((trade, timestamp) -> trade.getTime()))
                    .keyBy(Trade::getTicker)
                    .window(windowSize == slideBy ?
                            TumblingEventTimeWindows.of(Time.milliseconds(windowSize)) :
                            SlidingEventTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(slideBy)))
                    .aggregate(counting(), emitTimestampKeyAndCount())
                    .setParallelism(aggregationParallelism)
                    .flatMap(determineLatency())
                    .setParallelism(1);
            latencies
                    .map(t2 -> String.format("%d,%d", t2.f0, t2.f1))
                    .addSink(fileSink(outputPath, "latency-log"));
            latencies
                    .flatMap(recordLatencyHistogram(warmupSeconds, measurementSeconds))
                    .setParallelism(1)
                    .addSink(fileSink(outputPath, "latency-profile"));

        } catch (ValidationException | IOException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println("Reads trade events from a Kafka topic named \"" + KAFKA_TOPIC + "\", performs sliding");
            System.err.println("window aggregation on them and records the pipeline's latency:");
            System.err.println("how much after the window's end timestamp was Flink able to emit the first");
            System.err.println("key-value pair of the window result.");
            System.err.println();
            System.err.println("Usage:");
            System.err.println("    " + FlinkTradeMonitor.class.getSimpleName() + " [props-file]");
            System.err.println();
            System.err.println(
                    "The default properties file is " + DEFAULT_PROPERTIES_FILENAME + " in the current directory.");
            System.err.println();
            System.err.println("An example of the required properties:");
            System.err.println(PROP_BROKER_URI + "=localhost:9092");
            System.err.println("# earliest or latest:");
            System.err.println(PROP_OFFSET_RESET + "=latest");
            System.err.println(PROP_KAFKA_SOURCE_PARALLELISM + "=4");
            System.err.println(PROP_AGGREGATION_PARALLELISM + "=12");
            System.err.println(PROP_WINDOW_SIZE_MILLIS + "=10_000");
            System.err.println(PROP_SLIDING_STEP_MILLIS + "=100");
            System.err.println(PROP_CHECKPOINT_DATA_URI + "=file:/path/to/state-backend");
            System.err.println(PROP_CHECKPOINT_INTERVAL_MILLIS + "=10_000");
            System.err.println(PROP_ASYNCHRONOUS_SNAPSHOTS + "=true");
            System.err.println("# fs or rocksdb:");
            System.err.println(PROP_STATE_BACKEND + "=fs");
            System.err.println(PROP_WARMUP_SECONDS + "=40");
            System.err.println(PROP_MEASUREMENT_SECONDS + "=240");
            System.err.println(PROP_OUTPUT_PATH + "=benchmark-results");
            System.exit(1);
            return;
        }
        try {
            JobClient job = env.executeAsync("Trade Monitor Benchmark");
            AtomicBoolean jobCompletionFlag = new AtomicBoolean();
            Thread mainThread = currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                job.cancel().join();
                mainThread.interrupt();
                waitForCompletion(job, jobCompletionFlag);
            }));
            waitForCompletion(job, jobCompletionFlag);
        } catch (Exception e) {
            System.err.println("Job execution failed");
            e.printStackTrace();
        }
    }

    private static AggregateFunction<Trade, MutableLong, Long> counting() {
        return new AggregateFunction<Trade, MutableLong, Long>() {
            @Override
            public MutableLong createAccumulator () {
                return new MutableLong();
            }

            @Override
            public MutableLong add (Trade value, MutableLong accumulator){
                accumulator.increment();
                return accumulator;
            }

            @Override
            public MutableLong merge (MutableLong a, MutableLong b){
                a.setValue(Math.addExact(a.longValue(), b.longValue()));
                return a;
            }

            @Override
            public Long getResult (MutableLong accumulator){
                return accumulator.longValue();
            }
        };
    }

    private static WindowFunction<Long, Tuple3<Long, String, Long>, String, TimeWindow> emitTimestampKeyAndCount() {
        return new WindowFunction<Long, Tuple3<Long, String, Long>, String, TimeWindow>() {
            @Override
            public void apply(
                    String key, TimeWindow win, Iterable<Long> input, Collector<Tuple3<Long, String, Long>> out
            ) {
                out.collect(new Tuple3<>(win.getEnd(), key, input.iterator().next()));
            }
        };
    }

    private static FlatMapFunction<Tuple3<Long, String, Long>, Tuple2<Long, Long>> determineLatency() {
        return new FlatMapFunction<Tuple3<Long, String, Long>, Tuple2<Long, Long>>() {
            private long startTimestamp;
            private long lastTimestamp;

            @Override
            public void flatMap(Tuple3<Long, String, Long> tsKeyCount, Collector<Tuple2<Long, Long>> collector) {
                long timestamp = tsKeyCount.f0;
                if (timestamp <= lastTimestamp) {
                    return;
                }
                if (lastTimestamp == 0) {
                    startTimestamp = timestamp;
                }
                lastTimestamp = timestamp;

                long latency = System.currentTimeMillis() - timestamp;
                if (latency == -1) { // very low latencies may be reported as negative due to clock skew
                    latency = 0;
                }
                if (latency < 0) {
                    throw new RuntimeException("Negative latency: " + latency);
                }
                if (latency >= LATENCY_REPORTING_THRESHOLD_MS) {
                    System.out.format("Latency %,d ms (first seen key: %s, count %,d)%n",
                            latency, tsKeyCount.f1, tsKeyCount.f2);
                }
                collector.collect(new Tuple2<>(timestamp - startTimestamp, latency));
            }
        };
    }

    private static FlatMapFunction<Tuple2<Long, Long>, String> recordLatencyHistogram(
            long warmupTimeMillis, long totalTimeMillis
    ) {
        return new FlatMapFunction<Tuple2<Long, Long>, String>() {
            private Histogram histogram = new Histogram(5);

            @Override
            public void flatMap(Tuple2<Long, Long> timestampAndLatency, Collector<String> collector) {
                long timestamp = timestampAndLatency.f0;
                String timeMsg = String.format("%,d ", totalTimeMillis - timestamp);
                if (histogram == null) {
                    if (timestamp % BENCHMARKING_DONE_REPORT_INTERVAL_MS == 0) {
                        System.out.format(BENCHMARK_DONE_MESSAGE + " -- %s%n", timeMsg);
                    }
                    return;
                }
                if (timestamp < warmupTimeMillis) {
                    if (timestamp % WARMUP_REPORTING_INTERVAL_MS == 0) {
                        System.out.format("warming up -- %s%n", timeMsg);
                    }
                } else {
                    if (timestamp % MEASUREMENT_REPORTING_INTERVAL_MS == 0) {
                        System.out.println(timeMsg);
                    }
                    histogram.recordValue(timestampAndLatency.f1);
                }
                if (timestamp >= totalTimeMillis) {
                    try {
                        collector.collect(exportHistogram(histogram));
                    } finally {
                        histogram = null;
                    }
                }
            }
        };
    }

    private static String exportHistogram(Histogram histogram) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bos);
        histogram.outputPercentileDistribution(out, 1.0);
        out.close();
        return bos.toString();
    }

    private static StreamingFileSink<String> fileSink(String parent, String child) {
        return StreamingFileSink
                .forRowFormat(Path.fromLocalFile(new File(parent, child)),
                        new SimpleStringEncoder<String>())
                .build();
    }

    private static void waitForCompletion(JobClient job, AtomicBoolean jobCompletionFlag) {
        try {
            while (true) {
                JobStatus jobStatus = job.getJobStatus().get();
                if (jobStatus.isGloballyTerminalState()) {
                    if (!jobCompletionFlag.getAndSet(true)) {
                        System.out.println("Job terminal state: " + jobStatus);
                    }
                    return;
                }
                Thread.sleep(200);
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private static Properties createKafkaProperties(String brokerUrl, String offsetReset) {
        return props(
                "bootstrap.servers", brokerUrl,
                "group.id", UUID.randomUUID().toString(),
                "auto.offset.reset", offsetReset,
                "max.poll.records", "32768"
        );
    }

    private static DeserializationSchema<Trade> tradeDeserializationSchema() {
        return new AbstractDeserializationSchema<Trade>() {
            @Override
            public Trade deserialize(byte[] message) {
                return Util.deserializeTrade(message);
            }
        };
    }
}

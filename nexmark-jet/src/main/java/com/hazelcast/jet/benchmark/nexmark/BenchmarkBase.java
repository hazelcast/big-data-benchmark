package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.benchmark.nexmark.EventSourceP.simpleTime;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class BenchmarkBase {
    public static final String PROPS_FILENAME = "nexmark-jet.properties";
    public static final String PROP_EVENTS_PER_SECOND = "events-per-second";
    public static final String PROP_NUM_DISTINCT_KEYS = "num-distinct-keys";
    public static final String PROP_WINDOW_SIZE_MILLIS = "window-size-millis";
    public static final String PROP_SLIDING_STEP_MILLIS = "sliding-step-millis";
    public static final String PROP_WARMUP_SECONDS = "warmup-seconds";
    public static final String PROP_MEASUREMENT_SECONDS = "measurement-seconds";
    public static final String PROP_LATENCY_REPORTING_THRESHOLD_MILLIS = "latency-reporting-threshold-millis";
    public static final String PROP_OUTPUT_PATH = "output-path";

    static final long WARMUP_REPORTING_INTERVAL_MS = SECONDS.toMillis(2);
    static final long MEASUREMENT_REPORTING_INTERVAL_MS = SECONDS.toMillis(10);
    static final long BENCHMARKING_DONE_REPORT_INTERVAL_MS = SECONDS.toMillis(1);
    static final String BENCHMARK_DONE_MESSAGE = "benchmarking is done";
    static final long INITIAL_SOURCE_DELAY_MILLIS = 10;

    private final String benchmarkName;
    private int latencyReportingThresholdMs;

    BenchmarkBase(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Supply one argument: the simple class name of a benchmark");
            return;
        }
        String pkgName = BenchmarkBase.class.getPackage().getName();
        BenchmarkBase benchmark = (BenchmarkBase)
                Class.forName(pkgName + '.' + args[0]).newInstance();
        benchmark.run();
    }

    @SuppressWarnings("ConstantConditions")
    void run() {
        Properties props = loadProps();
        var jobCfg = new JobConfig();
        jobCfg.setName(benchmarkName);
        jobCfg.registerSerializer(Bid.class, Bid.BidSerializer.class);
        var jet = Jet.bootstrappedInstance();
        try {
            int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
            int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
            int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
            long slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
            int warmupSeconds = parseIntProp(props, PROP_WARMUP_SECONDS);
            int measurementSeconds = parseIntProp(props, PROP_MEASUREMENT_SECONDS);
            latencyReportingThresholdMs = parseIntProp(props, PROP_LATENCY_REPORTING_THRESHOLD_MILLIS);
            String outputPath = ensureProp(props, PROP_OUTPUT_PATH);
            System.out.printf(
                    "Benchmark name               %s%n" +
                    "Events per second            %,d%n" +
                    "Distinct keys                %,d%n" +
                    "Window size                  %,d ms%n" +
                    "Sliding step                 %,d ms%n" +
                    "Warmup period                %,d s%n" +
                    "Measurement period           %,d s%n" +
                    "Latency reporting threshold  %,d ms%n" +
                    "Output path                  %s%n",
                    benchmarkName,
                    eventsPerSecond,
                    numDistinctKeys,
                    windowSize,
                    slideBy,
                    warmupSeconds,
                    measurementSeconds,
                    latencyReportingThresholdMs,
                    outputPath
            );
            long warmupTimeMillis = SECONDS.toMillis(warmupSeconds);
            long totalTimeMillis = SECONDS.toMillis(warmupSeconds + measurementSeconds);

            var pipeline = Pipeline.create();
            var latencies = addComputation(pipeline, props);
            latencies.filter(t2 -> t2.f0() < totalTimeMillis)
                     .map(t2 -> String.format("%d,%d", t2.f0(), t2.f1()))
                     .writeTo(Sinks.files(new File(outputPath, "log").getPath()));
            latencies
                    .mapStateful(
                            () -> new RecordLatencyHistogram(warmupTimeMillis, totalTimeMillis),
                            RecordLatencyHistogram::map)
                    .writeTo(Sinks.files(new File(outputPath, "histogram").getPath()));

            var job = jet.newJob(pipeline, jobCfg);
            Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
            job.join();
        } catch (ValidationException e) {
            System.err.println(e.getMessage());
        }
    }

    abstract StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException;

    static Properties loadProps() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(PROPS_FILENAME));
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Can't read file " + PROPS_FILENAME);
            System.exit(2);
        }
        return props;
    }

    public static String ensureProp(Properties props, String propName) throws ValidationException {
        String prop = props.getProperty(propName);
        if (prop == null || prop.isEmpty()) {
            throw new ValidationException("Missing property: " + propName);
        }
        return prop;
    }

    public static int parseIntProp(Properties props, String propName) throws ValidationException {
        String prop = ensureProp(props, propName);
        try {
            return Integer.parseInt(prop.replace("_", ""));
        } catch (NumberFormatException e) {
            throw new ValidationException("Invalid property format, correct example is 9_999: " + propName + "=" + prop);
        }
    }

    <T> StreamStage<Tuple2<Long, Long>> determineLatency(
            StreamStage<T> stage,
            ToLongFunctionEx<? super T> timestampFn
    ) {
        int latencyReportingThresholdLocal = this.latencyReportingThresholdMs;
        return stage.mapStateful(
                () -> new DetermineLatency<>(timestampFn, latencyReportingThresholdLocal),
                DetermineLatency::map);
    }

    static long getRandom(long seq, long range) {
        return Math.abs(HashUtil.fastLongMix(seq)) % range;
    }

    static class DetermineLatency<T> {
        private final ToLongFunction<? super T> timestampFn;
        private final long latencyReportingThreshold;

        private long startTimestamp;
        private long lastTimestamp;

        DetermineLatency(ToLongFunction<? super T> timestampFn, long latencyReportingThreshold) {
            this.timestampFn = timestampFn;
            this.latencyReportingThreshold = latencyReportingThreshold;
        }

        Tuple2<Long, Long> map(T t) {
            long timestamp = timestampFn.applyAsLong(t);
            if (timestamp <= lastTimestamp) {
                return null;
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
            long time = simpleTime(timestamp);
            if (latency >= latencyReportingThreshold) {
                System.out.format("time %,d: latency %,d ms%n", time, latency);
            }
            return tuple2(timestamp - startTimestamp, latency);
        }
    }

    private static class RecordLatencyHistogram implements Serializable {
        private final long warmupTimeMillis;
        private final long totalTimeMillis;
        private long benchmarkDoneLastReport;
        private long warmingUpLastReport;
        private long measurementLastReport;
        private Histogram histogram = new Histogram(5);

        public RecordLatencyHistogram(long warmupTimeMillis, long totalTimeMillis) {
            this.warmupTimeMillis = warmupTimeMillis;
            this.totalTimeMillis = totalTimeMillis;
        }

        @SuppressWarnings("ConstantConditions")
        String map(Tuple2<Long, Long> timestampAndLatency) {
            long timestamp = timestampAndLatency.f0();
            String timeMsg = String.format("%,d ", totalTimeMillis - timestamp);
            if (histogram == null) {
                long benchmarkDoneNow = timestamp / BENCHMARKING_DONE_REPORT_INTERVAL_MS;
                if (benchmarkDoneNow > benchmarkDoneLastReport) {
                    benchmarkDoneLastReport = benchmarkDoneNow;
                    System.out.format(BENCHMARK_DONE_MESSAGE + " -- %s%n", timeMsg);
                }
                return null;
            }
            if (timestamp < warmupTimeMillis) {
                long warmingUpNow = timestamp / WARMUP_REPORTING_INTERVAL_MS;
                if (warmingUpNow > warmingUpLastReport) {
                    warmingUpLastReport = warmingUpNow;
                    System.out.format("warming up -- %s%n", timeMsg);
                }
            } else {
                long measurementNow = timestamp / MEASUREMENT_REPORTING_INTERVAL_MS;
                if (measurementNow > measurementLastReport) {
                    measurementLastReport = measurementNow;
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
}

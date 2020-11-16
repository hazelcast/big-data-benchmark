package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static com.hazelcast.jet.benchmark.nexmark.BidSourceP.bidSource;
import static com.hazelcast.jet.benchmark.nexmark.BidSourceP.simpleTime;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Query1 {
    private static final long EVENTS_PER_SECOND = 1_000_000;
    private static final String RESULTS_HOME = "/Users/mtopol/dev/java/big-data-benchmark/nexmark-jet/results/query1";

    private static final long WARMUP_TIME_MILLIS = SECONDS.toMillis(60);
    private static final long MEASUREMENT_TIME_MILLIS = MINUTES.toMillis(4);
    private static final long INITIAL_SOURCE_DELAY_MILLIS = 10;
    private static final long LATENCY_REPORTING_THRESHOLD = 7;
    private static final long TOTAL_TIME_MILLIS = WARMUP_TIME_MILLIS + MEASUREMENT_TIME_MILLIS;

    public static void main(String[] args) {
        System.out.printf(
                "%,d events per second%n" +
                "%,d milliseconds warmup%n" +
                "%,d milliseconds latency reporting threshold%n" +
                "%,d milliseconds source throughput reporting threshold%n",
                EVENTS_PER_SECOND, WARMUP_TIME_MILLIS,
                LATENCY_REPORTING_THRESHOLD, BidSourceP.SOURCE_THROUGHPUT_REPORTING_THRESHOLD
        );

        var jobCfg = new JobConfig();
        jobCfg.setName("Nexmark Query1");
        var jet = Jet.bootstrappedInstance();
        var job = jet.newJob(buildPipeline(), jobCfg);
        Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
        job.join();
    }

    @SuppressWarnings("ConstantConditions")
    static Pipeline buildPipeline() {
        var p = Pipeline.create();
        StreamStage<Bid> input = p
                .readFrom(bidSource(EVENTS_PER_SECOND, INITIAL_SOURCE_DELAY_MILLIS))
                .withNativeTimestamps(0);

        StreamStage<Bid> output = input
                .map(bid -> new Bid(bid.seq(), bid.timestamp(), bid.auctionId(), bid.price() * 8 / 10));

        StreamStage<Tuple2<Long, Long>> latencies = output
                .filter(bid -> bid.seq() % 1024 == 0)
                .mapStateful(DetermineLatency::new, DetermineLatency::map);

        latencies.filter(t2 -> t2.f0() < TOTAL_TIME_MILLIS)
                 .map(t2 -> String.format("%d,%d", t2.f0(), t2.f1()))
                 .writeTo(Sinks.files(RESULTS_HOME + "/log"));
        latencies
                .mapStateful(RecordLatencyHistogram::new, RecordLatencyHistogram::map)
                .writeTo(Sinks.files(RESULTS_HOME + "/histo"));

        return p;
    }

    private static class DetermineLatency {
        private long startTimestamp;
        private long lastTimestamp;

        Tuple2<Long, Long> map(Bid bid) {
            long timestamp = bid.timestamp();
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
            if (latency >= LATENCY_REPORTING_THRESHOLD) {
                System.out.format("time %,d: latency %,d ms%n", time, latency);
            }
            return tuple2(timestamp - startTimestamp, latency);
        }
    }

    private static class RecordLatencyHistogram {
        private Histogram histogram = new Histogram(5);

        @SuppressWarnings("ConstantConditions")
        String map(Tuple2<Long, Long> timestampAndLatency) {
            long timestamp = timestampAndLatency.f0();
            String timeMsg = String.format("%,d ", TOTAL_TIME_MILLIS - timestamp);
            if (histogram == null) {
                if (timestamp % 1_000 == 0) {
                    System.out.format("benchmarking is done -- %s%n", timeMsg);
                }
                return null;
            }
            if (timestamp < WARMUP_TIME_MILLIS) {
                if (timestamp % 2_000 == 0) {
                    System.out.format("warming up -- %s%n", timeMsg);
                }
            } else {
                if (timestamp % 10_000 == 0) {
                    System.out.println(timeMsg);
                }
                histogram.recordValue(timestampAndLatency.f1());
            }
            if (timestamp >= TOTAL_TIME_MILLIS) {
                try {
                    return exportHistogram(histogram);
                } finally {
                    histogram = null;
                }
            }
            return null;
        }
    }

    private static String exportHistogram(Histogram histogram) {
        var bos = new ByteArrayOutputStream();
        var out = new PrintStream(bos);
        histogram.outputPercentileDistribution(out, 1.0);
        out.close();
        return bos.toString();
    }
}

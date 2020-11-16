package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.benchmark.nexmark.model.Bid;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BidSourceP extends AbstractProcessor {
    static final long SOURCE_THROUGHPUT_REPORTING_THRESHOLD = 30_000_000;

    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS = 10;
    private static final long SIMPLE_TIME_SPAN_MILLIS = HOURS.toMillis(3);
    private static final long THROUGHPUT_REPORT_PERIOD_NANOS =
            MILLISECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS);
    private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 10;

    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long startTime;
    private final long itemsPerSecond;
    private final boolean isReportingThroughput;
    private final long wmGranularity;
    private final long wmOffset;
    private long globalProcessorIndex;
    private long totalParallelism;
    private long emitPeriod;

    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);
    private long emitSchedule;
    private long lastReport;
    private long counterAtLastReport;
    private long lastCallNanos;
    private long counter;
    private long lastEmittedWm;
    private long nowNanos;

    BidSourceP(
            long startTime,
            long itemsPerSecond,
            EventTimePolicy<? super Bid> eventTimePolicy,
            boolean shouldReportThroughput
    ) {
        this.wmGranularity = eventTimePolicy.watermarkThrottlingFrameSize();
        this.wmOffset = eventTimePolicy.watermarkThrottlingFrameOffset();
        this.startTime = MILLISECONDS.toNanos(startTime + nanoTimeMillisToCurrentTimeMillis);
        this.itemsPerSecond = itemsPerSecond;
        this.isReportingThroughput = shouldReportThroughput;
    }

    @SuppressWarnings("SameParameterValue")
    public static StreamSource<Bid> bidSource(long itemsPerSecond, long initialDelay) {
        return Sources.streamFromProcessorWithWatermarks("longs", true, eventTimePolicy -> ProcessorMetaSupplier.of(
                (Address ignored) -> {
                    long startTime = System.currentTimeMillis() + initialDelay;
                    return ProcessorSupplier.of(() ->
                            new BidSourceP(startTime, itemsPerSecond, eventTimePolicy, true));
                })
        );
    }

    public static long simpleTime(long timeMillis) {
        return timeMillis % SIMPLE_TIME_SPAN_MILLIS;
    }

    private static long determineTimeOffset() {
        long milliTime = System.currentTimeMillis();
        long nanoTime = System.nanoTime();
        return NANOSECONDS.toMillis(nanoTime) - milliTime;
    }

    @Override
    protected void init(Context context) {
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        emitPeriod = SECONDS.toNanos(1) * totalParallelism / itemsPerSecond;
        lastCallNanos = lastReport = emitSchedule =
                startTime + SECONDS.toNanos(1) * globalProcessorIndex / itemsPerSecond;
    }

    @Override
    public boolean complete() {
        nowNanos = System.nanoTime();
        emitEvents();
        detectAndReportHiccup();
        if (isReportingThroughput) {
            reportThroughput();
        }
        return false;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void emitEvents() {
        while (emitFromTraverser(traverser) && emitSchedule <= nowNanos) {
            long timestamp = NANOSECONDS.toMillis(emitSchedule) - nanoTimeMillisToCurrentTimeMillis;
            Bid bid = new Bid(
                    counter * totalParallelism + globalProcessorIndex,
                    timestamp,
                    0,
                    100
            );
            traverser.append(jetEvent(timestamp, bid));
            counter++;
            emitSchedule += emitPeriod;
            if (timestamp >= lastEmittedWm + wmGranularity) {
                long wmToEmit = timestamp - (timestamp % wmGranularity) + wmOffset;
                traverser.append(new Watermark(wmToEmit));
                lastEmittedWm = wmToEmit;
            }
        }
    }

    private void detectAndReportHiccup() {
        long millisSinceLastCall = NANOSECONDS.toMillis(nowNanos - lastCallNanos);
        if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
            System.out.printf("*** Source #%d hiccup: %,d ms%n", globalProcessorIndex, millisSinceLastCall);
        }
        lastCallNanos = nowNanos;
    }

    private void reportThroughput() {
        long nanosSinceLastReport = nowNanos - lastReport;
        if (nanosSinceLastReport < THROUGHPUT_REPORT_PERIOD_NANOS) {
            return;
        }
        lastReport = nowNanos;
        long itemCountSinceLastReport = counter - counterAtLastReport;
        counterAtLastReport = counter;
        double throughput = itemCountSinceLastReport / ((double) nanosSinceLastReport / SECONDS.toNanos(1));
        if (throughput >= (double) SOURCE_THROUGHPUT_REPORTING_THRESHOLD / totalParallelism) {
            System.out.printf("%,d p%d: %,.0f items/second%n",
                    simpleTime(NANOSECONDS.toMillis(nowNanos)),
                    globalProcessorIndex,
                    throughput
            );
        }
    }

    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        throw new UnsupportedOperationException("Source processor shouldn't be asked to process a watermark");
    }
}


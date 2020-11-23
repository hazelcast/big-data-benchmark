package com.hazelcast.jet.benchmark.nexmark;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.BiFunctionEx;
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

public class EventSourceP extends AbstractProcessor {
    static final long SOURCE_THROUGHPUT_REPORTING_THRESHOLD = 30_000_000;

    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS = 10;
    private static final long SIMPLE_TIME_SPAN_MILLIS = HOURS.toMillis(3);
    private static final long THROUGHPUT_REPORT_PERIOD_NANOS =
            MILLISECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS);
    private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 10;

    private final long itemsPerSecond;
    private final long startTime;
    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long wmGranularity;
    private final long wmOffset;
    private final BiFunctionEx<? super Long, ? super Long, ?> createEventFn;
    private int globalProcessorIndex;
    private int totalParallelism;
    private long emitPeriod;

    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);
    private long emitSchedule;
    private long lastReport;
    private long counterAtLastReport;
    private long lastCallNanos;
    private long counter;
    private long lastEmittedWm;
    private long nowNanos;

    <T> EventSourceP(
            long startTime,
            long itemsPerSecond,
            EventTimePolicy<? super T> eventTimePolicy,
            BiFunctionEx<? super Long, ? super Long, ? extends T> createEventFn
    ) {
        this.startTime = MILLISECONDS.toNanos(startTime + nanoTimeMillisToCurrentTimeMillis);
        this.itemsPerSecond = itemsPerSecond;
        this.createEventFn = createEventFn;
        wmGranularity = eventTimePolicy.watermarkThrottlingFrameSize();
        wmOffset = eventTimePolicy.watermarkThrottlingFrameOffset();
    }

    @Override
    protected void init(Context context) {
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        emitPeriod = SECONDS.toNanos(1) * totalParallelism / itemsPerSecond;
        lastCallNanos = lastReport = emitSchedule =
                startTime + SECONDS.toNanos(1) * globalProcessorIndex / itemsPerSecond;
    }

    @SuppressWarnings("SameParameterValue")
    public static <T> StreamSource<T> eventSource(
            long eventsPerSecond, long initialDelayMs,
            BiFunctionEx<? super Long, ? super Long, ? extends T> createEventFn
    ) {
        return Sources.streamFromProcessorWithWatermarks("longs", true, eventTimePolicy -> ProcessorMetaSupplier.of(
                (Address ignored) -> {
                    long startTime = System.currentTimeMillis() + initialDelayMs;
                    return ProcessorSupplier.of(() ->
                            new EventSourceP(
                                    startTime, eventsPerSecond, eventTimePolicy, createEventFn
                            ));
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
    public boolean complete() {
        nowNanos = System.nanoTime();
        emitEvents();
        detectAndReportHiccup();
        reportThroughput();
        return false;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void emitEvents() {
        while (emitFromTraverser(traverser) && emitSchedule <= nowNanos) {
            long timestamp = NANOSECONDS.toMillis(emitSchedule) - nanoTimeMillisToCurrentTimeMillis;
            long seq = counter * totalParallelism + globalProcessorIndex;
            Object event = createEventFn.apply(seq, timestamp);
            traverser.append(jetEvent(timestamp, event));
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


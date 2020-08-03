package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RealTimeTradeProducer implements Runnable {

    public enum MessageType {
        BYTE,
        OBJECT
    }

    private static final int BATCH_SIZE = 1_024;
    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);
    private static final long REPORT_PERIOD_SECONDS = 2;

    private final int producerIndex;
    private final String topic;
    private final int tradesPerSecond;
    private final String[] tickers;
    private final KafkaProducer<Long, Object> producer;
    private final MessageType messageType;

    private long startNanoTime;
    private int tickerIndex;
    private long producedCount = 0;
    private long lastReportNanoTime;
    private long nowNanos;
    private long producedAtLastReport = 0;
    private long latestTimestampNanoTime = 0;


    private RealTimeTradeProducer(
            int producerIndex, String broker, String topic, int tradesPerSecond,
            int keysFrom, int keysTo, MessageType messageType
    ) {
        if (tradesPerSecond <= 0) {
            throw new RuntimeException("tradesPerSecond=" + tradesPerSecond);
        }
        this.producerIndex = producerIndex;
        this.topic = topic;
        this.tradesPerSecond = tradesPerSecond;
        this.messageType = messageType;

        tickers = new String[keysTo - keysFrom];
        Arrays.setAll(tickers, i -> "T-" + (i + keysFrom));
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        switch (messageType) {
            case BYTE:
                props.setProperty("value.serializer", ByteArraySerializer.class.getName());
                break;
            case OBJECT:
                props.setProperty("value.serializer", TradeSerializer.class.getName());
                break;
            default:
                throw new RuntimeException("Missing implementation for message type " + messageType);
        }
        this.producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("Usage:");
            System.err.println("  " + RealTimeTradeProducer.class.getSimpleName()
                    + " <bootstrap.servers> <topic> <num producers> <trades per second>" +
                    " <num distinct keys> <messageType>");
            System.err.println();
            System.err.println("<messageType> - byte|object");
            System.exit(1);
        }
        String broker = args[0];
        String topic = args[1];
        int numProducers = Integer.parseInt(args[2].replace("_", ""));
        int tradesPerSecond = Integer.parseInt(args[3].replace("_", ""));
        int numDistinctKeys = Integer.parseInt(args[4].replace("_", ""));
        MessageType messageType = MessageType.valueOf(args[5].toUpperCase());

        int keysPerProducer = numDistinctKeys / numProducers;
        int tradesPerSecondPerProducer = tradesPerSecond / numProducers;
        if (keysPerProducer * numProducers *100 / numDistinctKeys < 99) {
            System.err.println("<num distinct keys> not divisible by <num producers> and error is >1%");
            System.exit(1);
        }
        if (tradesPerSecondPerProducer * numProducers *100 / tradesPerSecond < 99) {
            System.err.println("<trades per second> not divisible by <num producers> and error is >1%");
            System.exit(1);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(numProducers);
        for (int i = 0; i < numProducers; i++) {
            RealTimeTradeProducer tradeProducer = new RealTimeTradeProducer(i, broker, topic,
                    tradesPerSecondPerProducer, keysPerProducer * i, keysPerProducer * (i + 1), messageType);
            executorService.submit(tradeProducer);
        }
    }

    @Override
    public void run() {
        if (tradesPerSecond == -1) {
            runNonThrottled();
            return; // should never be reached
        }
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream oos = new DataOutputStream(baos);
        final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();

        startNanoTime = System.nanoTime();
        lastReportNanoTime = startNanoTime;
        while (true) {
            nowNanos = System.nanoTime();
            reportThroughput();
            final long expectedProduced = (nowNanos - startNanoTime) * tradesPerSecond / NANOS_PER_SECOND;
            if (sleepIfAheadOfSchedule(expectedProduced)) {
                continue;
            }
            for (int i = 0; producedCount < expectedProduced && i < BATCH_SIZE; i++) {
                long timestampNanoTime = startNanoTime + producedCount * NANOS_PER_SECOND / tradesPerSecond;
                long timestamp = NANOSECONDS.toMillis(timestampNanoTime) - nanoTimeMillisToCurrentTimeMillis;
                send(baos, oos, topic, nextTrade(timestamp), messageType);
                producedCount++;
                latestTimestampNanoTime = timestampNanoTime;
            }
        }
    }

    private void reportThroughput() {
        final long nanosSinceLastReport = nowNanos - lastReportNanoTime;
        if (NANOSECONDS.toSeconds(nanosSinceLastReport) >= REPORT_PERIOD_SECONDS) {
            System.out.printf("Producer %2d: topic '%s', %,.0f events/second, %,d ms behind real time%n",
                    producerIndex, topic,
                    (double) NANOS_PER_SECOND * (producedCount - producedAtLastReport) / nanosSinceLastReport,
                    NANOSECONDS.toMillis(nowNanos - latestTimestampNanoTime));
            producedAtLastReport = producedCount;
            lastReportNanoTime = nowNanos;
        }
    }

    private boolean sleepIfAheadOfSchedule(long expectedProduced) {
        if (expectedProduced == producedCount) {
            long nextSchedule = startNanoTime + (producedCount + 1) * NANOS_PER_SECOND / tradesPerSecond;
            long nanosLeftToNextEvent = nextSchedule - nowNanos;
            long sleepNanos = nanosLeftToNextEvent - MICROSECONDS.toNanos(10);
            if (sleepNanos > 0) {
                LockSupport.parkNanos(sleepNanos);
                return true;
            }
        }
        return false;
    }

    private void runNonThrottled() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream oos = new DataOutputStream(baos);

        long start = System.nanoTime();
        long produced = 0;
        while (true) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                send(baos, oos, topic, nextTrade(System.currentTimeMillis()), messageType);
            }
            produced += BATCH_SIZE;

            long elapsed = System.nanoTime() - start;
            double rate = produced / (double) TimeUnit.NANOSECONDS.toSeconds(elapsed);
            System.out.println(producerIndex + ": Produced: " + rate + " trades/sec");
        }
    }

    private Trade nextTrade(long time) {
        String ticker = tickers[tickerIndex++];
        if (tickerIndex == tickers.length) {
            tickerIndex = 0;
        }
        return new Trade(time, ticker, 100, 10000);
    }

    private void send(
            ByteArrayOutputStream baos, DataOutputStream oos, String topic, Trade trade, MessageType messageType
    ) {
        Object msgObject;
        switch (messageType) {
            case BYTE:
                msgObject = serialize(baos, oos, trade);
                break;
            case OBJECT:
                msgObject = trade;
                break;
            default:
                throw new RuntimeException("Missing implementation for message type " + messageType);
        }
        producer.send(new ProducerRecord<>(topic, msgObject));
    }

    private byte[] serialize(ByteArrayOutputStream baos, DataOutputStream oos, Trade trade) {
        try {
            oos.writeUTF(trade.getTicker());
            oos.writeLong(trade.getTime());
            oos.writeInt(trade.getPrice());
            oos.writeInt(trade.getQuantity());
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            baos.reset();
        }
    }

    private static long determineTimeOffset() {
        return NANOSECONDS.toMillis(System.nanoTime()) - System.currentTimeMillis();
    }
}

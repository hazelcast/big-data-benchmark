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
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RealTimeTradeProducer implements Runnable {
    public enum MessageType {
        BYTE,
        OBJECT
    }

    private static final int BATCH_SIZE = 256;
    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);
    private static final long REPORT_PERIOD_SECONDS = 2;

    private final int producerIndex;
    private final String topic;
    private final double tradesPerNanosecond;
    private final long nanoTimeMillisToCurrentTimeMillis;
    private final String[] tickers;
    private final KafkaProducer<Long, Object> producer;
    private final MessageType messageType;
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final DataOutputStream oos = new DataOutputStream(baos);

    private long startNanoTime;
    private int tickerIndex;
    private long producedCount = 0;
    private long lastReportNanoTime;
    private long nowNanos;
    private long producedAtLastReport = 0;
    private long latestTimestampNanoTime = 0;


    private RealTimeTradeProducer(
            int producerIndex, String broker, String topic, double tradesPerSecond,
            int keysPerProducer, MessageType messageType, long nanoTimeMillisToCurrentTimeMillis
    ) {
        if (tradesPerSecond <= 0) {
            throw new RuntimeException("tradesPerSecond=" + tradesPerSecond);
        }
        this.producerIndex = producerIndex;
        this.topic = topic;
        this.tradesPerNanosecond = tradesPerSecond / NANOS_PER_SECOND;
        this.messageType = messageType;
        this.nanoTimeMillisToCurrentTimeMillis = nanoTimeMillisToCurrentTimeMillis;

        int keysFrom = keysPerProducer * producerIndex;
        int keysTo = keysPerProducer * (producerIndex + 1);
        tickers = new String[keysTo - keysFrom];
        Arrays.setAll(tickers, i -> "T-" + (keysFrom + i));

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
        producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("Usage:");
            System.err.println("  " + RealTimeTradeProducer.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <num producers> <trades per second>" +
                    " <num distinct keys> <messageType>");
            System.err.println();
            System.err.println("<messageType> - byte|object");
            System.exit(1);
        }
        String broker = args[0];
        String topic = args[1];
        int numProducers = parseIntArg(args[2]);
        int tradesPerSecond = parseIntArg(args[3]);
        int numDistinctKeys = parseIntArg(args[4]);
        MessageType messageType = MessageType.valueOf(args[5].toUpperCase());

        if (tradesPerSecond < 1) {
            System.err.println("<trades per second> must be positive, but got " + tradesPerSecond);
            System.exit(1);
        }
        int keysPerProducer = numDistinctKeys / numProducers;
        double tradesPerSecondPerProducer = (double) tradesPerSecond / numProducers;
        if (keysPerProducer * numProducers * 100 / numDistinctKeys < 99) {
            System.err.println("<num distinct keys> not divisible by <num producers> and the error is >1%");
            System.exit(1);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(numProducers);
        long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
        for (int i = 0; i < numProducers; i++) {
            RealTimeTradeProducer tradeProducer = new RealTimeTradeProducer(i, broker, topic,
                    tradesPerSecondPerProducer, keysPerProducer, messageType, nanoTimeMillisToCurrentTimeMillis);
            executorService.submit(tradeProducer);
        }
    }

    private static int parseIntArg(String arg) {
        return Integer.parseInt(arg.replace("_", ""));
    }

    @Override
    public void run() {
        startNanoTime = System.nanoTime();
        lastReportNanoTime = startNanoTime;
        while (true) {
            nowNanos = System.nanoTime();
            long expectedProduced = (long) ((nowNanos - startNanoTime) * tradesPerNanosecond);
            if (producedCount < expectedProduced) {
                produceUntil(expectedProduced);
                reportThroughput();
            } else {
                sleepUntilDue(expectedProduced + 1);
            }
        }
    }

    private void sleepUntilDue(long expectedProduced) {
        long due = startNanoTime + (long) (expectedProduced / tradesPerNanosecond);
        long nanosUntilDue = due - nowNanos;
        long sleepNanos = nanosUntilDue - MICROSECONDS.toNanos(10);
        if (sleepNanos > 0) {
            LockSupport.parkNanos(sleepNanos);
        }
    }

    private void produceUntil(long expectedProduced) {
        for (int i = 0; producedCount < expectedProduced && i < BATCH_SIZE; i++) {
            long timestampNanoTime = startNanoTime + (long) (producedCount / tradesPerNanosecond);
            long timestamp = NANOSECONDS.toMillis(timestampNanoTime) - nanoTimeMillisToCurrentTimeMillis;
            send(baos, oos, topic, nextTrade(timestamp), messageType);
            producedCount++;
            latestTimestampNanoTime = timestampNanoTime;
        }
    }

    private void reportThroughput() {
        final long nanosSinceLastReport = nowNanos - lastReportNanoTime;
        if (NANOSECONDS.toSeconds(nanosSinceLastReport) < REPORT_PERIOD_SECONDS) {
            return;
        }
        System.out.printf("Producer %2d: topic '%s', %,.0f events/second, %,d ms behind real time%n",
                producerIndex, topic,
                (double) NANOS_PER_SECOND * (producedCount - producedAtLastReport) / nanosSinceLastReport,
                NANOSECONDS.toMillis(nowNanos - latestTimestampNanoTime));
        producedAtLastReport = producedCount;
        lastReportNanoTime = nowNanos;
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

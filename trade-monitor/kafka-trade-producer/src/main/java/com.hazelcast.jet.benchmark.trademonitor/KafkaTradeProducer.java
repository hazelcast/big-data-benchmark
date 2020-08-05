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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaTradeProducer implements Runnable {
    public enum MessageType {
        BYTE,
        OBJECT
    }

    public static final String KAFKA_TOPIC = "trades";
    private static final int BATCH_SIZE = 256;
    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);
    private static final long REPORT_PERIOD_SECONDS = 2;

    private final int producerIndex;
    private final double tradesPerNanosecond;
    private final long startNanoTime;
    private final long nanoTimeMillisToCurrentTimeMillis;
    private final String[] tickers;
    private final KafkaProducer<Long, Object> producer;
    private final MessageType messageType;
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final DataOutputStream oos = new DataOutputStream(baos);

    private int tickerIndex;
    private long lastReportNanoTime;
    private long nowNanos;
    private long producedCount;
    private long producedAtLastReport;
    private long latestTimestampNanoTime;


    private KafkaTradeProducer(
            int producerIndex, int producerCount, String broker, double tradesPerSecond,
            int keysPerProducer, MessageType messageType, long startNanoTime, long nanoTimeMillisToCurrentTimeMillis
    ) {
        if (tradesPerSecond <= 0) {
            throw new RuntimeException("tradesPerSecond=" + tradesPerSecond);
        }
        this.producerIndex = producerIndex;
        this.tradesPerNanosecond = tradesPerSecond / NANOS_PER_SECOND;
        this.messageType = messageType;
        this.startNanoTime = (long) (startNanoTime + ((double) producerIndex / producerCount) / tradesPerNanosecond);
        this.lastReportNanoTime = startNanoTime;
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
        if (args.length != 5) {
            System.err.println("Usage:");
            System.err.println("  " + KafkaTradeProducer.class.getSimpleName() +
                    " <bootstrap.servers> <num producers> <trades per second>" +
                    " <num distinct keys> <messageType>");
            System.err.println();
            System.err.println("<messageType> - byte|object");
            System.err.println("You can use _ in the numbers, for example 1_000_000.");
            System.exit(1);
        }
        String broker = args[0];
        int numProducers = parseIntArg(args[1]);
        int tradesPerSecond = parseIntArg(args[2]);
        int numDistinctKeys = parseIntArg(args[3]);
        MessageType messageType = MessageType.valueOf(args[4].toUpperCase());

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
        long startNanoTime = System.nanoTime() + MILLISECONDS.toNanos(100);
        for (int i = 0; i < numProducers; i++) {
            KafkaTradeProducer tradeProducer = new KafkaTradeProducer(i, numProducers, broker,
                    tradesPerSecondPerProducer, keysPerProducer, messageType,
                    startNanoTime, nanoTimeMillisToCurrentTimeMillis);
            executorService.submit(tradeProducer);
        }
    }

    private static int parseIntArg(String arg) {
        return Integer.parseInt(arg.replace("_", ""));
    }

    @Override
    public void run() {
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
            send(baos, oos, KAFKA_TOPIC, nextTrade(timestamp), messageType);
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
                producerIndex, KAFKA_TOPIC,
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

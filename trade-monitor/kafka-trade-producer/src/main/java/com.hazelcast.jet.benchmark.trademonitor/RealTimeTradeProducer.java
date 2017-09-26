package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RealTimeTradeProducer implements Runnable {

    public static final int BATCH_SIZE = 100_000;
    private final KafkaProducer<Long, Trade> producer;
    private final int index;
    private final String topic;
    private final int tradesPerSecond;
    private String[] tickers;
    private int tickerIndex;

    private RealTimeTradeProducer(int index, String broker, String topic, int tradesPerSecond, int keysFrom, int keysTo) throws IOException,
            URISyntaxException {
        if (tradesPerSecond <= 0) {
            throw new RuntimeException("tradesPerSecond=" + tradesPerSecond);
        }
        this.index = index;
        this.topic = topic;
        this.tradesPerSecond = tradesPerSecond;
        tickers = new String[keysTo - keysFrom];
        Arrays.setAll(tickers, i -> "T-" + Integer.toString(i + keysFrom));
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", TradeSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage:");
            System.err.println("  " + RealTimeTradeProducer.class.getSimpleName() + " <bootstrap.servers> <topic> <num producers> <trades per second> <num distinct keys>");
            System.exit(1);
        }
        String broker = args[0];
        String topic = args[1];
        int numProducers = Integer.parseInt(args[2]);
        int tradesPerSecond = Integer.parseInt(args[3]);
        int numDistinctKeys = Integer.parseInt(args[4]);

        ExecutorService executorService = Executors.newFixedThreadPool(numProducers);
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
        for (int i = 0; i < numProducers; i++) {
            RealTimeTradeProducer tradeProducer = new RealTimeTradeProducer(i, broker, topic, tradesPerSecondPerProducer,
                    keysPerProducer * i, keysPerProducer * (i + 1));
            executorService.submit(tradeProducer);
        }
    }

    private void send(String topic, Trade trade) {
        producer.send(new ProducerRecord<>(topic, null, null, trade));
    }

    private void runNonThrottled() {
        long start = System.nanoTime();
        long produced = 0;
        while (true) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                send(topic, nextTrade(System.currentTimeMillis()));
            }
            produced += BATCH_SIZE;

            long elapsed = System.nanoTime() - start;
            double rate = produced / (double) TimeUnit.NANOSECONDS.toSeconds(elapsed);
            System.out.println(index + ": Produced: " + rate + " trades/ sec");
        }
    }

    @Override
    public void run() {
        if (tradesPerSecond == -1) {
            runNonThrottled();
        }

        final long start = System.nanoTime();
        long totalTradesProduced = 0;
        for (long second = 0; ; second++) {
            for (long j = 0, k = 0; j < tradesPerSecond; j++, k++) {
                send(topic, nextTrade(System.currentTimeMillis()));
                totalTradesProduced++;
                if (k == Math.min(100, tradesPerSecond / 10)) {
                    long expectedTimeMs = second * 1000 + j * 1000 / tradesPerSecond;
                    long sleepTime = start + MILLISECONDS.toNanos(expectedTimeMs) - System.nanoTime();
                    LockSupport.parkNanos(sleepTime);
                    k = 0;
                }
            }

            long timeSinceStart = System.nanoTime() - start;
            long expectedProduced = (long) (tradesPerSecond * (double) timeSinceStart / SECONDS.toNanos(1));
            System.out.println(String.format("%2d: Produced %d trades to topic '%s', current production deficit=%d",
                    index, tradesPerSecond, topic, (expectedProduced - totalTradesProduced)));
        }
    }

    private Trade nextTrade(long time) {
        String ticker = tickers[tickerIndex++];
        if (tickerIndex == tickers.length) {
            tickerIndex = 0;
        }
//        lag++;
//        if (lag == 2000) {
//            lag = 0;
//        }
        return new Trade(time, ticker, 100, 10000);
    }
}

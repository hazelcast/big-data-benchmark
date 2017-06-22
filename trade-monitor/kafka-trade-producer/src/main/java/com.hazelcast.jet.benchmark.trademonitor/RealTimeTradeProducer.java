package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RealTimeTradeProducer implements Runnable {

    public static final int BATCH_SIZE = 100_000;
    private final KafkaProducer<Long, Trade> producer;
    private final int index;
    private final String topic;
    private final int tradesPerSecond;
    private Map<String, Integer> tickersToPrice = new HashMap<>();
    private String[] tickers;
    private int tickerIndex;
    private long lag;

    private RealTimeTradeProducer(int index, String broker, String topic, int tradesPerSecond, long numDistinctKeys) throws IOException,
            URISyntaxException {
        this.index = index;
        this.topic = topic;
        this.tradesPerSecond = tradesPerSecond;
        loadTickers(numDistinctKeys);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", TradeSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage:");
            System.err.println("  "+RealTimeTradeProducer.class+" <bootstrap.servers> <topic> <num producers> <trades per second> <num distinct keys>");
            System.exit(1);
        }
        String broker = args[0];
        String topic = args[1];
        int numProducers = Integer.parseInt(args[2]);
        int tradesPerSecond = Integer.parseInt(args[3]);
        int numDistinctKeys = Integer.parseInt(args[4]);

        ExecutorService executorService = Executors.newFixedThreadPool(numProducers);
        for (int i = 0; i < numProducers; i++) {
            RealTimeTradeProducer tradeProducer = new RealTimeTradeProducer(i, broker, topic, tradesPerSecond, numDistinctKeys);
            executorService.submit(tradeProducer);
        }
    }

    private void send(String topic, Trade trade) {
        producer.send(new ProducerRecord<>(topic, null, null, trade));
    }

    public void runUnthrottled() {
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
            runUnthrottled();
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
            System.out.println(index + ": Produced " + tradesPerSecond + " trades to topic '" + topic + '\''
                    + ", current production deficit=" + (expectedProduced - totalTradesProduced));
        }
    }

    private void loadTickers(long numDistinctKeys) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TradeProducer.class.getResourceAsStream
                ("/nasdaqlisted.txt")))) {
            Stream<String> lines = reader.lines();
            lines.skip(1).limit(numDistinctKeys).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put
                    (t, 10000));
            tickers = tickersToPrice.keySet().toArray(new String[0]);
        } catch (Exception e) {
            throw new RuntimeException(e);
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

package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class RealTimeTradeProducer {

    private final KafkaProducer<Long, Trade> producer;
    private Map<String, Integer> tickersToPrice = new HashMap<>();
    private String[] tickers;
    private int tickerIndex;
    private long lag;

    private RealTimeTradeProducer(String broker, int tradesPerSecond) throws IOException, URISyntaxException {
        loadTickers(tradesPerSecond);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", TradeSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage:");
            System.err.println("  TradeProducer <bootstrap.servers> <topic> <trades per second> <stop after seconds>");
            System.exit(1);
        }
        String broker = args[0];
        String topic = args[1];
        int tradesPerSecond = Integer.parseInt(args[2]);
        long numSeconds = Long.parseLong(args[3]);

        RealTimeTradeProducer tradeProducer = new RealTimeTradeProducer(broker, tradesPerSecond);

        final long start = System.nanoTime();
        long totalTradesProduced = 0;
        for (long second = 0; second < numSeconds; second++) {
            for (long j = 0, k = 0; j < tradesPerSecond; j++, k++) {
                Trade trade = tradeProducer.nextTrade(System.currentTimeMillis());
                tradeProducer.send(topic, trade);
                totalTradesProduced++;
                if (k == Math.min(100, tradesPerSecond-1)) {
                    long expectedTimeMs = second * 1000 + j * 1000 / tradesPerSecond;
                    long sleepTime = start + MILLISECONDS.toNanos(expectedTimeMs) - System.nanoTime();
                    LockSupport.parkNanos(sleepTime);
                    k = 0;
                }
            }

            System.out.println("Produced " + tradesPerSecond + " trades to topic '" + topic + '\''
                    + ", current lag=" + (tradesPerSecond * NANOSECONDS.toMillis(System.nanoTime() - start) / 1000 - totalTradesProduced));
        }
        tradeProducer.close();
    }

    private void send(String topic, Trade trade) {
        producer.send(new ProducerRecord<>(topic, trade));
    }

    private void close() {
        producer.flush();
        producer.close();
    }

    private void loadTickers(long tradesPerSecond) throws URISyntaxException, IOException {
        Stream<String> lines = Files.lines(Paths.get(RealTimeTradeProducer.class.getResource("/nasdaqlisted.txt").toURI()));
        lines.skip(1).limit(tradesPerSecond).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put(t, 10000));
        tickers = tickersToPrice.keySet().toArray(new String[0]);
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

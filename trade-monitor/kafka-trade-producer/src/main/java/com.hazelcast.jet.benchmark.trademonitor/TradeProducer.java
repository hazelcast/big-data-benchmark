package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import sun.misc.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class TradeProducer {

    private final KafkaProducer<Long, Trade> producer;
    private Map<String, Integer> tickersToPrice = new HashMap<>();
    private String[] tickers;
    private int tickerIndex;
    private long lag;

    private TradeProducer(String broker) throws IOException, URISyntaxException {
        loadTickers();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", TradeSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage:");
            System.err.println("  TradeProducer <bootstrap.servers> <topic> <trades per second> <number of seconds>");
            System.exit(1);
        }
        String broker = args[0];
        String topic = args[1];
        long tradesPerSecond = Long.parseLong(args[2]);
        long numSeconds = Long.parseLong(args[3]);

        long start = System.currentTimeMillis();
        TradeProducer tradeProducer = new TradeProducer(broker);

        for (long i = 0; i < numSeconds; i++) {
            tradeProducer.produce(topic, i, tradesPerSecond);
        }
        tradeProducer.close();
        long time = System.currentTimeMillis() - start;
        double records = tradesPerSecond * numSeconds;
        System.out.println("Produced " + (long)records + " records in " + time + " seconds. Average rate: " + records
                /time*1000);
    }

    private void close() {
        producer.flush();
        producer.close();
    }

    private void produce(String topic, long time, long count) {
        System.out.println("Producing second " + time);
        for (long i = 0; i < count; i++) {
            Trade trade = nextTrade(time);
            producer.send(new ProducerRecord<>(topic, trade));
        }
    }

    private void loadTickers() throws URISyntaxException, IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TradeProducer.class.getResourceAsStream
                ("/nasdaqlisted.txt")))) {
            reader.lines().skip(1).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put(t, 10000));
            tickers = tickersToPrice.keySet().toArray(new String[0]);
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

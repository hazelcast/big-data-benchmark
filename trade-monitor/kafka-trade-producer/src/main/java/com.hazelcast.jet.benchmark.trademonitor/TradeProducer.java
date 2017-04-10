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
        String broker = args[0];
        String topic = args[1];
        long tradersPerSecond = Long.parseLong(args[2]);
        int numSeconds = 1000;

        TradeProducer tradeProducer = new TradeProducer(broker);

        for (int i = 0; i < numSeconds; i++) {
            tradeProducer.produce(topic, i, tradersPerSecond);
        }
        tradeProducer.close();
    }

    private void close() {
        producer.flush();
        producer.close();
    }

    private void produce(String topic, long time, long count) {
        for (long i = 0; i < count; i++) {
            Trade trade = nextTrade(time);
            producer.send(new ProducerRecord<>(topic, trade));
        }
        System.out.println("Produced " + count + " trades to topic " + topic);
    }

    private void loadTickers() throws URISyntaxException, IOException {
        Stream<String> lines = Files.lines(Paths.get(TradeProducer.class.getResource("/nasdaqlisted.txt").toURI()));
        lines.skip(1).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put(t, 10000));
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

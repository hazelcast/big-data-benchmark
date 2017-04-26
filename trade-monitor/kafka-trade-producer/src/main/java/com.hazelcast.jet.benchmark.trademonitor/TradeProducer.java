package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TradeProducer implements AutoCloseable {

    private static long SEC_TO_NANOS = TimeUnit.SECONDS.toNanos(1);

    private KafkaProducer<Long, Trade> producer;
    private Map<String, Integer> tickersToPrice = new HashMap<>();
    private String[] tickers;
    private int tickerIndex;
    private long lag;

    private TradeProducer(String broker) {
        loadTickers();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", TradeSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage:");
            System.err.println("  TradeProducer <bootstrap.servers> <topic> <num producers> <trades per second> " +
                    "<number of seconds>");
            System.exit(1);
        }


        String broker = args[0];
        String topic = args[1];
        int numProducers = Integer.parseInt(args[2]);
        long tradesPerSecond = Long.parseLong(args[3]);
        long numSeconds = Long.parseLong(args[4]);

        ExecutorService service = Executors.newCachedThreadPool();

        AtomicLong totalProduced = new AtomicLong();
        final long start = System.nanoTime();
        CyclicBarrier barrier = new CyclicBarrier(numProducers);
        for (int j = 0; j < numProducers; j++) {
            int producerIdx = j;
            service.submit(() -> {
                try (TradeProducer tradeProducer = new TradeProducer(broker)) {
                    for (long i = 0; i < numSeconds; i++) {
                        long batchStart = System.nanoTime();
                        tradeProducer.produce(topic, i, tradesPerSecond);
                        totalProduced.addAndGet(tradesPerSecond);
                        long producerProduced = i * tradesPerSecond;
                        long batchEnd = System.nanoTime();
                        long batchElapsed = batchEnd - batchStart;
                        long totalElapsed = batchEnd - start;
                        long batchRate = (long) ((double) tradesPerSecond / batchElapsed * SEC_TO_NANOS);
                        long totalRate = (long) ((double) totalProduced.get() / totalElapsed * SEC_TO_NANOS);
                        System.out.printf("Producer %d: Produced %,d records. Producer rate: %,d records/s%n",
                                producerIdx, producerProduced,
                                TimeUnit.NANOSECONDS.toMillis(totalElapsed), batchRate);
                        System.out.printf("Aggregate: Produced %,d records. Total rate: %,d records/s%n",
                                totalProduced.get(), totalRate);
                        try {
                            barrier.await();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
        }
        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private void produce(String topic, long time, long count) {
        for (long i = 0; i < count; i++) {
            Trade trade = nextTrade(time);
            producer.send(new ProducerRecord<>(topic, trade));
        }
    }

    private void loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TradeProducer.class.getResourceAsStream
                ("/nasdaqlisted.txt")))) {
            reader.lines().skip(1).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put(t, 10000));
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

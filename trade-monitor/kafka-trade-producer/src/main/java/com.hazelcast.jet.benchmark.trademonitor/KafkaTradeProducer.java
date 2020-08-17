package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.benchmark.Util;
import com.hazelcast.jet.benchmark.ValidationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.benchmark.Util.ensureProp;
import static com.hazelcast.jet.benchmark.Util.loadProps;
import static com.hazelcast.jet.benchmark.Util.parseIntProp;
import static com.hazelcast.jet.benchmark.Util.props;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaTradeProducer implements Runnable {

    public static final String DEFAULT_PROPERTIES_FILENAME = "kafka-trade-producer.properties";
    public static final String PROP_KAFKA_BROKER_URI = "kafka-broker-uri";
    public static final String PROP_NUM_PARALLEL_PRODUCERS = "num-parallel-producers";
    public static final String PROP_TRADES_PER_SECOND = "trades-per-second";
    public static final String PROP_NUM_DISTINCT_KEYS = "num-distinct-keys";
    public static final String KAFKA_TOPIC = "trades";

    private static final int BATCH_SIZE = 256;
    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);
    private static final long REPORT_PERIOD_SECONDS = 2;

    private final int threadIndex;
    private final double tradesPerNanosecond;
    private final long startNanoTime;
    private final long nanoTimeMillisToCurrentTimeMillis;
    private final KafkaProducer<String, Trade> kafkaProducer;
    private final String[] tickers;

    private int currentTickerIndex;
    private long lastReportNanoTime;
    private long nowNanos;
    private long producedCount;
    private long producedAtLastReport;
    private long latestTimestampNanoTime;

    private KafkaTradeProducer(
            int threadIndex, int numThreads,
            KafkaProducer<String, Trade> kafkaProducer,
            double tradesPerSecond, String[] tickers,
            long startNanoTime, long nanoTimeMillisToCurrentTimeMillis
    ) {
        if (tradesPerSecond <= 0) {
            throw new RuntimeException("tradesPerSecond = " + tradesPerSecond);
        }
        this.kafkaProducer = kafkaProducer;
        this.threadIndex = threadIndex;
        this.tradesPerNanosecond = tradesPerSecond / NANOS_PER_SECOND;
        this.startNanoTime = (long) (startNanoTime + ((double) threadIndex / numThreads) / tradesPerNanosecond);
        this.lastReportNanoTime = startNanoTime;
        this.nanoTimeMillisToCurrentTimeMillis = nanoTimeMillisToCurrentTimeMillis;
        this.tickers = tickers;
    }

    public static void main(String[] args) {
        String propsPath = args.length > 0 ? args[0] : DEFAULT_PROPERTIES_FILENAME;
        Properties props = loadProps(propsPath);
        try {
            String brokerUri = ensureProp(props, PROP_KAFKA_BROKER_URI);
            int numThreads = parseIntProp(props, PROP_NUM_PARALLEL_PRODUCERS);
            int tradesPerSecond = parseIntProp(props, PROP_TRADES_PER_SECOND);
            int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
            Util.printParams(
                    PROP_KAFKA_BROKER_URI, brokerUri,
                    PROP_NUM_PARALLEL_PRODUCERS, numThreads,
                    PROP_TRADES_PER_SECOND, tradesPerSecond,
                    PROP_NUM_DISTINCT_KEYS, numDistinctKeys
            );
            if (tradesPerSecond < 1) {
                System.err.println(PROP_TRADES_PER_SECOND + " must be positive, but got " + tradesPerSecond);
                System.exit(1);
            }
            String[][] tickersByThread = assignTickersToThreads(numThreads, numDistinctKeys);
            Properties kafkaProps = props(
                    "bootstrap.servers", brokerUri,
                    "key.serializer", IntegerSerializer.class.getName(),
                    "value.serializer", TradeSerializer.class.getName()
            );
            double tradesPerSecondPerProducer = (double) tradesPerSecond / numThreads;
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
            long startNanoTime = System.nanoTime() + MILLISECONDS.toNanos(100);
            for (int i = 0; i < numThreads; i++) {
                KafkaProducer<String, Trade> kafkaProducer = new KafkaProducer<>(kafkaProps);
                KafkaTradeProducer tradeProducer = new KafkaTradeProducer(i, numThreads, kafkaProducer,
                        tradesPerSecondPerProducer, tickersByThread[i],
                        startNanoTime, nanoTimeMillisToCurrentTimeMillis);
                executorService.submit(tradeProducer);
            }
            executorService.shutdown();
        } catch (ValidationException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println("Usage:");
            System.err.println("  " + KafkaTradeProducer.class.getSimpleName() + " [props-file]");
            System.err.println();
            System.err.println(
                    "The default properties file is " + DEFAULT_PROPERTIES_FILENAME + " in the current directory");
            System.err.println("An example of the required properties:");
            System.err.println(
                    "    " + PROP_KAFKA_BROKER_URI + "=localhost:9092\n" +
                    "    " + PROP_NUM_PARALLEL_PRODUCERS + "=1\n" +
                    "    " + PROP_TRADES_PER_SECOND + "=1_000_000\n" +
                    "    " + PROP_NUM_DISTINCT_KEYS + "=10_000");
            System.err.println();
            System.err.println(
                    "The program emits the given number of " + PROP_TRADES_PER_SECOND + " to the\n" +
                    "Kafka topic \"" + KAFKA_TOPIC + "\", and uses " + PROP_NUM_PARALLEL_PRODUCERS + " threads\n" +
                    "to do it. Every thread runs its own instance of a Kafka Producer client\n" +
                    "and produces its share of the requested events per second, using its\n" +
                    "distinct share of the requested keyset size. Each producer sends the\n" +
                    "data to its own Kafka partition ID, equal to the zero-based index of the\n" +
                    "producer.\n");
            System.err.println(
                    "The trade event timestamps are predetermined and don't depend on the\n" +
                    "current time. Effectively, this program simulates a constant stream of\n" +
                    "equally-spaced trade events. It guarantees it won't try to send an event\n" +
                    "to Kafka before it has occurred, but there's no guarantee on how much\n" +
                    "later it will manage to send it. If the requested throughput is too\n" +
                    "high, the producer may be increasingly falling back behind real time.\n" +
                    "The timestamps it emits will still be the same, but this delay in\n" +
                    "sending the events contributes to the reported end-to-end latency. You\n" +
                    "can track this in the program's output.\n"
            );
            System.exit(1);
        }
    }

    @Override
    public void run() {
        try {
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
        } catch (Exception e) {
            System.err.println("Producer #" + threadIndex + " failed");
            e.printStackTrace();
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
            send(nextTrade(timestamp));
            producedCount++;
            latestTimestampNanoTime = timestampNanoTime;
        }
    }

    private void reportThroughput() {
        final long nanosSinceLastReport = nowNanos - lastReportNanoTime;
        if (NANOSECONDS.toSeconds(nanosSinceLastReport) < REPORT_PERIOD_SECONDS) {
            return;
        }
        System.out.printf("Producer %d: %,.0f events/second, %,d ms behind real time%n",
                threadIndex,
                (double) NANOS_PER_SECOND * (producedCount - producedAtLastReport) / nanosSinceLastReport,
                NANOSECONDS.toMillis(nowNanos - latestTimestampNanoTime));
        producedAtLastReport = producedCount;
        lastReportNanoTime = nowNanos;
    }

    private Trade nextTrade(long time) {
        try {
            return new Trade(time, tickers[currentTickerIndex], 100, 10000);
        } finally {
            if (++currentTickerIndex == tickers.length) {
                currentTickerIndex = 0;
            }
        }
    }

    private void send(Trade trade) {
        kafkaProducer.send(new ProducerRecord<>(KAFKA_TOPIC, threadIndex, trade.getTime(), null, trade));
    }

    private static long determineTimeOffset() {
        return NANOSECONDS.toMillis(System.nanoTime()) - System.currentTimeMillis();
    }

    private static String[][] assignTickersToThreads(int numThreads, int numDistinctKeys) {
        List<String> tickers = generateTickers(numDistinctKeys);
        String[][] tickersByThread = new String[numThreads][];
        double tickersPerThread = (double) numDistinctKeys / numThreads;
        for (int i = 0; i < numThreads; i++) {
            int lowerBound = (int) Math.round(i * tickersPerThread);
            int upperBound = (int) Math.round((i + 1) * tickersPerThread);
            tickersByThread[i] = new String[upperBound - lowerBound];
            for (int j = lowerBound; j < upperBound; j++) {
                tickersByThread[i][j - lowerBound] = tickers.get(j);
            }
        }
        return tickersByThread;
    }

    private static List<String> generateTickers(int numTickers) {
        int alphabetSize = 'Z' - 'A' + 1;
        int countToGenerate = alphabetSize;
        int numLetters = 1;
        while (numTickers > countToGenerate) {
            if (numLetters == 5) {
                throw new IllegalArgumentException("Asked for too many tickers, max is " + countToGenerate);
            }
            countToGenerate *= alphabetSize;
            numLetters++;
        }
        char[] tickerLetters = new char[numLetters];
        Arrays.fill(tickerLetters, 'A');
        List<String> all = new ArrayList<>(countToGenerate);
        for (int i = 0; i < countToGenerate; i++) {
            all.add(new String(tickerLetters));
            for (int j = 0; j < numLetters; j++) {
                tickerLetters[j]++;
                if (tickerLetters[j] <= 'Z') {
                    break;
                }
                tickerLetters[j] = 'A';
            }
        }
        Collections.shuffle(all);
        return new ArrayList<>(all.subList(0, numTickers));
    }
}

package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class TradeTestConsumer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.partition.fetch.bytes", "1024");
        KafkaConsumer<Long, Trade> consumer = new KafkaConsumer<>(props);
        List<String> topics = Arrays.asList(args[1]);
        consumer.subscribe(topics);
        System.out.println("Subscribed to topics " + topics);
        long count = 0;
        long start = System.nanoTime();
        while (true) {
            ConsumerRecords<Long, Trade> poll = consumer.poll(5000);
            System.out.println("Partitions in batch: " + poll.partitions());
            LongSummaryStatistics stats = StreamSupport.stream(poll.spliterator(), false)
                                                                       .mapToLong(r -> r.value().getTime()).summaryStatistics();
            System.out.println("Oldest record time: " + stats.getMin() + ", newest record: " + stats.getMax());
            count += poll.count();
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long rate = (long) ((double) count / elapsed * 1000);
            System.out.printf("Total count: %,d in %,dms. Average rate: %,d records/s %n", count, elapsed, rate);

        }
    }

}

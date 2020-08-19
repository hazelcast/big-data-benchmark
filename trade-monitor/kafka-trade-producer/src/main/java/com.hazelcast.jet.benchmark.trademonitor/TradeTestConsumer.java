package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.benchmark.Trade;
import com.hazelcast.jet.benchmark.Util;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.LongSummaryStatistics;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.benchmark.Util.props;
import static java.util.Collections.singletonList;

public class TradeTestConsumer {

    public static void main(String[] args) {
        Properties props = props(
                "bootstrap.servers", args[0],
                "group.id", UUID.randomUUID().toString(),
                "key.deserializer", LongDeserializer.class.getName(),
                "value.deserializer", KafkaTradeDeserializer.class.getName(),
                "auto.offset.reset", "earliest");
        KafkaConsumer<Long, Trade> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singletonList(Util.KAFKA_TOPIC));
        System.out.println("Subscribed to topic " + Util.KAFKA_TOPIC);
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

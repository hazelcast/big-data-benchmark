package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;

public class TradeTestConsumer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "1024");
        KafkaConsumer<Long, Trade> consumer = new KafkaConsumer<>(props);
        List<String> topics = Arrays.asList(args[1]);
        consumer.subscribe(topics);
        System.out.println("Subscribed to topics " + topics);
        long count = 0;
        while (true) {
            ConsumerRecords<Long, Trade> poll = consumer.poll(5000);
            System.out.println(poll.partitions());
            LongSummaryStatistics longSummaryStatistics = StreamSupport.stream(poll.spliterator(), false)
                                                                       .mapToLong(r -> r.value().getTime()).summaryStatistics();
            System.out.println(longSummaryStatistics);
            count += longSummaryStatistics.getCount();
            System.out.println("Total count: " + count);
        }
    }

}

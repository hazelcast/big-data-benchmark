package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TradeConsumer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", "1231231");
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "32768");
        KafkaConsumer<Long, Trade> consumer = new KafkaConsumer<>(props);
        List<String> topics = Arrays.asList(args[2]);
        consumer.subscribe(topics);
        System.out.println("Subscribed to topics " + topics);
        long count = 0;
        while (true) {
            ConsumerRecords<Long, Trade> poll = consumer.poll(5000);
            System.out.println("Polled " + poll.count() + " records. Total consumed = " + (count += poll.count()));
        }
    }

}

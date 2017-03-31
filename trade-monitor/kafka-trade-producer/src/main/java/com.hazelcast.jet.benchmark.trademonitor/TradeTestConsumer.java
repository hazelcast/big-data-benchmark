package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class TradeTestConsumer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", "2");
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "1000");
        KafkaConsumer<Long, Trade> consumer = new KafkaConsumer<>(props);
        List<String> topics = Arrays.asList(args[1]);
        consumer.subscribe(topics);
        System.out.println("Subscribed to topics " + topics);
        long count = 0;
        while (true) {
            ConsumerRecords<Long, Trade> poll = consumer.poll(5000);
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (ConsumerRecord<Long, Trade> r : poll) {
                min = Math.min(r.value().getTime(), min);
                max = Math.max(r.value().getTime(), max);
            }
            System.out.println("Polled " + poll.count() + " records. Total consumed = " + (count += poll.count()) + "" +
                    " min=" +min + " max=" + max);
        }
    }

}

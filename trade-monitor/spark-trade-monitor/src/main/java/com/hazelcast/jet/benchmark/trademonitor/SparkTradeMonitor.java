/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.benchmark.trademonitor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SparkTradeMonitor {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage:");
            System.err.println("  SparkTradeMonitor <bootstrap.servers> <topic> <checkpoint directory>");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        final String brokerUri = args[0];
        final String topic = args[1];
        final String checkpointDirectory = args[2];

//        JetInstance instance = Jet.newJetInstance();
//        IStreamList<Frame<String, Long>> sinkList = instance.getList("sink");

        SparkConf conf = new SparkConf()
                .setAppName("Trade Monitor")
                .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.checkpoint(checkpointDirectory);

        final JavaInputDStream<ConsumerRecord<String, Trade>> stream =
                KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Trade>Subscribe(Collections.singleton(topic), getKafkaProperties(brokerUri)));

        JavaPairDStream<String, Long> paired = stream.mapToPair(record -> new Tuple2<>(record.key(), 1L));
        JavaPairDStream<String, Long> reduced = paired.reduceByKeyAndWindow(
                        (Long a, Long b) -> a + b,
                        (Long a, Long b) -> a - b,
                        Durations.seconds(10),
                        Durations.seconds(1));
        reduced.foreachRDD(rdd -> {
                    rdd.saveAsTextFile("c:/tmp/tradesOutput");
                });

        jsc.start();

//        final JavaInputDStream<ConsumerRecord<String, String>> stream =
//                KafkaUtils.createDirectStream(jsc,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//                );
//
//        JavaRDD<String> textFile = sc.textFile(args[0]);
//        JavaPairRDD<String, Long> pairs =
//                words.mapToPair((PairFunction<String, String, Long>) s -> new Tuple2<>(s, 1L));
//
//        System.out.println("Starting task..");
//        long t = System.currentTimeMillis();
//        counts.saveAsTextFile(args[1] + "_" + t);
//        System.out.println("Time=" + (System.currentTimeMillis() - t));
    }

    private static Map<String, Object> getKafkaProperties(String brokerUrl) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", brokerUrl);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", TradeDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
//        props.put("max.poll.records", "32768");
        return props;
    }
}

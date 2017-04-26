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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SparkLatencyMonitor {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 4) {
            System.err.println("Usage:");
            System.err.println("  SparkTradeMonitor <bootstrap.servers> <topic> <checkpoint directory> <output file>");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        final String brokerUri = args[0];
        final String topic = args[1];
        final String checkpointDirectory = args[2];
        final String outputFile = args[3];

        SparkConf conf = new SparkConf()
                .setAppName("Trade Monitor")
                .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.checkpoint(checkpointDirectory);

        final JavaInputDStream<ConsumerRecord<String, Trade>> stream =
                KafkaUtils.createDirectStream(jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Trade>Subscribe(Collections.singleton(topic), getKafkaProperties(brokerUri)));
        JavaPairDStream<String, Long> paired = stream.mapToPair(record -> new Tuple2<>(record.value().getTicker(), 1L));
        JavaPairDStream<String, Long> reduced = paired.reduceByKeyAndWindow(
                (Long a, Long b) -> a + b,
                (Long a, Long b) -> a - b,
                Durations.seconds(10),
                Durations.seconds(1));
        reduced.foreachRDD((rdd, time) -> appendToFile(outputFile,
                "time=" + time.milliseconds()
                + ", latency=" + (System.currentTimeMillis() - time.milliseconds())
                + ", count=" + rdd.count()));

        jsc.start();
        jsc.awaitTermination();
    }

    private static void appendToFile(String fileName, String value) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(Paths.get(fileName), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(value);
            w.newLine();
        }
    }

    private static Map<String, Object> getKafkaProperties(String brokerUrl) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", brokerUrl);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.deserializer", LongDeserializer.class);
        props.put("value.deserializer", TradeDeserializer.class);
        props.put("auto.offset.reset", "latest");
        return props;
    }
}
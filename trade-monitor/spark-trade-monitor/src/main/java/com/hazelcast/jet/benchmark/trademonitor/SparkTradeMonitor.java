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
import scala.Tuple3;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SparkTradeMonitor {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 7) {
            System.err.println("Usage:");
            System.err.println("  SparkTradeMonitor <bootstrap.servers> <topic> <microbatchLength> <windowSize> " +
                    "<slideBy> <output file> <checkpointDir>");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        int microbatchLength = Integer.parseInt(args[2]);
        int windowSize = Integer.parseInt(args[3]);
        int slideBy = Integer.parseInt(args[4]);
        String outputPath = args[5];
        String checkpointDir = args[6];

        SparkConf conf = new SparkConf()
//                .setMaster("local[8]")
                .setAppName("Trade Monitor");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.milliseconds(microbatchLength));
        if (checkpointDir.length() > 0) {
            jsc.checkpoint(checkpointDir);
        }

        final JavaInputDStream<ConsumerRecord<String, Trade>> stream =
                KafkaUtils.createDirectStream(jsc,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.<String, Trade>Subscribe(Collections.singleton(topic), getKafkaProperties(brokerUri)));
        JavaPairDStream<String, Long> paired = stream.mapToPair(record -> new Tuple2<>(record.value().getTicker(), 1L));
        JavaPairDStream<String, Long> reduced = paired.reduceByKeyAndWindow(
                (Long a, Long b) -> a + b,
                (Long a, Long b) -> a - b,
                Durations.milliseconds(windowSize),
                Durations.milliseconds(slideBy));

        reduced
                .transform((r, time) -> r.map(t ->
                        System.currentTimeMillis() - time.milliseconds()))
                .dstream().saveAsTextFiles(outputPath + "/t", "");

        jsc.start();
        jsc.awaitTermination();
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

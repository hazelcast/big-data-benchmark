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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;


public class FlinkTradeMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage:");
            System.err.println("  FlinkTradeMonitor <bootstrap.servers> <topic> <windowSize> <slideBy> <output_path>");
            System.exit(1);
        }

        String brokerUri = args[0];
        String topic = args[1];
        int windowSize = Integer.parseInt(args[2]);
        int slide = Integer.parseInt(args[3]);
        String outputPath = args[4];

        // set up the execution environment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DeserializationSchema<Trade> schema = new AbstractDeserializationSchema<Trade>() {
            TradeDeserializer deserializer = new TradeDeserializer();

            @Override
            public Trade deserialize(byte[] message) throws IOException {
                return deserializer.deserialize(null, message);
            }
        };

        DataStreamSource<Trade> trades = env.addSource(new FlinkKafkaConsumer010<>(topic,
                schema, getKafkaProperties(brokerUri))).setParallelism(16);
        AssignerWithPeriodicWatermarks<Trade> timestampExtractor
                = new BoundedOutOfOrdernessTimestampExtractor<Trade>(Time.milliseconds(1)) {
            @Override
            public long extractTimestamp(Trade element) {
                return element.getTime();
            }
        };

        WindowAssigner window =
                windowSize == slide ?
                        TumblingEventTimeWindows.of(Time.milliseconds(windowSize)) :
                SlidingEventTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds
                (slide));
        trades
                .assignTimestampsAndWatermarks(timestampExtractor)
                .keyBy((Trade t) -> t.getTicker())
                .window(window)
                .fold(0L, new FoldFunction<Trade, Long>() {
                    @Override public Long fold(Long accumulator, Trade value) throws Exception {
                        return accumulator + 1;
                    }
                }, new WindowFunction<Long, Tuple4<Long, String, Long, Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window,
                                      Iterable<Long> input,
                                      Collector<Tuple4<Long, String, Long, Long>> out) throws Exception {
                        Long count = input.iterator().next();
                        out.collect(new Tuple4<>(window.getEnd(), key, count, System.currentTimeMillis()));
                    }
                })
                .writeAsCsv(outputPath, WriteMode.OVERWRITE);

        JobExecutionResult execute = env.execute("Trade Monitor Example");
    }

    private static Properties getKafkaProperties(String brokerUrl) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "32768");
        return props;
    }
}
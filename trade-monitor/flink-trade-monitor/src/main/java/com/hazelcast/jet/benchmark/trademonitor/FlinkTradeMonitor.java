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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.UUID;


public class FlinkTradeMonitor {

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("flink-output"));

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
        DataStreamSource<Trade> trades = env.addSource(new FlinkKafkaConsumer010<>("trades",
                schema, getKafkaProperties("localhost:9092")));
        AssignerWithPeriodicWatermarks<Trade> timestampExtractor
                = new BoundedOutOfOrdernessTimestampExtractor<Trade>(Time.milliseconds(10)) {
            @Override
            public long extractTimestamp(Trade element) {
                return element.getTime();
            }
        };

        trades
                .assignTimestampsAndWatermarks(timestampExtractor)
                .map(t -> new Tuple2<>(t.getTicker(), 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(1000), Time.milliseconds(1)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> left, Tuple2<String, Long> right) throws
                            Exception {
                        return new Tuple2<>(left.f0, left.f1 + right.f1);
                    }
                }, new WindowFunction<Tuple2<String,Long>, Tuple4<Long,String,Long, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window,
                                      Iterable<Tuple2<String, Long>> input,
                                      Collector<Tuple4<Long,String,Long, Long>> out) throws Exception {
                        Tuple2<String, Long> val = input.iterator().next();
                        Tuple4<Long, String, Long, Long> t = new Tuple4<>(window.getEnd(), val.f0, val.f1, System
                                .currentTimeMillis());
                        out.collect(t);
                    }
                })
                .writeAsCsv("flink-output");

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
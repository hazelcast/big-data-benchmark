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

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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
        if (args.length != 13) {
            System.err.println("Usage:");
            System.err.println("  " + FlinkTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs> <outputPath> <checkpointInterval> <checkpointUri> <doAsyncSnapshot> <stateBackend> <kafkaParallelism> <windowParallelism>");
            System.err.println("<stateBackend> - fs | rocksDb");
            System.exit(1);
        }
        String brokerUri = args[0];
        String topic = args[1];
        String offsetReset = args[2];
        int lagMs = Integer.parseInt(args[3]);
        int windowSize = Integer.parseInt(args[4]);
        int slideBy = Integer.parseInt(args[5]);
        String outputPath = args[6];
        int checkpointInt = Integer.parseInt(args[7]);
        String checkpointUri = args[8];
        boolean doAsyncSnapshot = Boolean.parseBoolean(args[9]);
        String stateBackend = args[10];
        int kafkaParallelism = Integer.parseInt(args[11]);
        int windowParallelism = Integer.parseInt(args[12]);

        System.out.println("bootstrap.servers: " + brokerUri);
        System.out.println("topic: " + topic);
        System.out.println("offset-reset: " + offsetReset);
        System.out.println("lag: " + lagMs);
        System.out.println("windowSize: " + windowSize);
        System.out.println("slideBy: " + slideBy);
        System.out.println("outputPath: " + outputPath);
        System.out.println("checkpointInt: " + checkpointInt);
        System.out.println("checkpointUri: " + checkpointUri);
        System.out.println("doAsyncSnapshot: " + doAsyncSnapshot);
        System.out.println("stateBackend: " + stateBackend);
        System.out.println("kafkaParallelism: " + kafkaParallelism);
        System.out.println("windowParallelism: " + windowParallelism);

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        if (checkpointInt > 0) {
            env.enableCheckpointing(checkpointInt);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInt);
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 5000));
        if ("fs".equalsIgnoreCase(stateBackend)) {
            env.setStateBackend(new FsStateBackend(checkpointUri, doAsyncSnapshot));
        } else if ("rocksDb".equalsIgnoreCase(stateBackend)) {
            env.setStateBackend(new RocksDBStateBackend(checkpointUri));
        } else {
            System.err.println("Bad value for stateBackend: " + stateBackend);
            System.exit(1);
        }

        DeserializationSchema<Trade> schema = new AbstractDeserializationSchema<Trade>() {
            TradeDeserializer deserializer = new TradeDeserializer();

            @Override
            public Trade deserialize(byte[] message) throws IOException {
                return deserializer.deserialize(null, message);
            }
        };

        DataStreamSource<Trade> trades = env.addSource(new FlinkKafkaConsumer010<>(topic,
                schema, getKafkaProperties(brokerUri, offsetReset))).setParallelism(kafkaParallelism);
        AssignerWithPeriodicWatermarks<Trade> timestampExtractor
                = new BoundedOutOfOrdernessTimestampExtractor<Trade>(Time.milliseconds(lagMs)) {
            @Override
            public long extractTimestamp(Trade element) {
                return element.getTime();
            }
        };

        WindowAssigner<Object, TimeWindow> window = windowSize == slideBy ?
                TumblingEventTimeWindows.of(Time.milliseconds(windowSize)) :
                SlidingEventTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(slideBy));

        trades
                .assignTimestampsAndWatermarks(timestampExtractor)
                .keyBy(Trade::getTicker)
                .window(window)
                .aggregate(new AggregateFunction<Trade, MutableLong, Long>() {
                    @Override
                    public MutableLong createAccumulator() {
                        return new MutableLong();
                    }

                    @Override
                    public MutableLong add(Trade value, MutableLong accumulator) {
                        accumulator.increment();
                        return accumulator;
                    }

                    @Override
                    public MutableLong merge(MutableLong a, MutableLong b) {
                        a.setValue(Math.addExact(a.longValue(), b.longValue()));
                        return a;
                    }

                    @Override
                    public Long getResult(MutableLong accumulator) {
                        return accumulator.longValue();
                    }
                }, new WindowFunction<Long, Long, String, TimeWindow>() {
                    @Override public void apply(String key, TimeWindow win, Iterable<Long> input, Collector<Long> out) {
                        out.collect(System.currentTimeMillis() - win.getEnd() - lagMs);
                    }
                })
                .setParallelism(windowParallelism)
                .writeAsText(outputPath, WriteMode.OVERWRITE);

        env.execute("Trade Monitor Example");
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }
}

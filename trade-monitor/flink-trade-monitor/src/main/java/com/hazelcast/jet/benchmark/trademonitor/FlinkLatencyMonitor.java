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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
import java.util.concurrent.atomic.AtomicLong;

public class FlinkLatencyMonitor {

    private static final AtomicLong totalSum = new AtomicLong();
    private static final AtomicLong totalCount = new AtomicLong();

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage:");
            System.err.println("  "+FlinkLatencyMonitor.class.getSimpleName()+" <bootstrap.servers> <topic> <slideBy> [<outputFile>]");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        int slideBy = Integer.parseInt(args[2]);
        String fileName = args.length > 3 ? args[3] : null;

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
                schema, getKafkaProperties(brokerUri)));
        AssignerWithPeriodicWatermarks<Trade> timestampExtractor
                = new BoundedOutOfOrdernessTimestampExtractor<Trade>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(Trade element) {
                return element.getTime();
            }
        };

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> finalStream = trades
                .assignTimestampsAndWatermarks(timestampExtractor)
                .keyBy(Trade::getTicker)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(10000), Time.milliseconds(slideBy)))
                .fold(new Tuple2<MutableLong, MutableLong>(new MutableLong(), new MutableLong()),
                        new FoldFunction<Trade, Tuple2<MutableLong, MutableLong>>() {
                                @Override
                                public Tuple2<MutableLong, MutableLong> fold(
                                        Tuple2<MutableLong, MutableLong> accumulator, Trade trade
                                ) {
                                    accumulator.f0.setValue(Math.addExact(accumulator.f0.longValue(), trade.getPrice()));
                                    accumulator.f1.increment();
                                    return accumulator;
                                }
                }, new WindowFunction<Tuple2<MutableLong, MutableLong>, Tuple3<Long, String, Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window,
                            Iterable<Tuple2<MutableLong, MutableLong>> input,
                            Collector<Tuple3<Long, String, Long>> out) throws Exception {
                        Tuple2<MutableLong, MutableLong> avgPriceAcc = input.iterator().next();
                        long avgPrice = avgPriceAcc.f0.longValue() / avgPriceAcc.f1.longValue();
                        out.collect(new Tuple3<>(window.getEnd(), key, avgPrice));
                    }
                });
        finalStream
                .addSink(new RichSinkFunction<Tuple3<Long, String, Long>>() {
                    @Override
                    public void invoke(Tuple3<Long, String, Long> value) throws Exception {
                        long latency = System.currentTimeMillis() - value.f0;
                        //logger.info("sink1-frame drained=" + drainedCount + " frames, now=" + System.nanoTime() + ", iterationCnt=" + iterationCnt);

                        totalSum.addAndGet(latency);
                        totalCount.addAndGet(1);
                    }
                });
        if (fileName != null) {
            finalStream
                    .writeAsCsv(fileName, WriteMode.OVERWRITE);
        }

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
                long sum = totalSum.get();
                long count = totalCount.get();
                totalSum.set(0);
                totalCount.set(0);
                System.out.println("average latency=" + (count != 0 ? sum / count + "ms" : "?") + ", count=" + count);
            }
        }).start();

        JobExecutionResult execute = env.execute("Trade Monitor Example");
    }

    private static Properties getKafkaProperties(String brokerUrl) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("max.poll.records", "32768");
        return props;
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.benchmark.wordcount;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.StringTokenizer;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.SECONDS;


public class FlinkWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage:");
            System.err.println("  " + FlinkWordCount.class.getSimpleName() + " <inputPath> <outputPath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1] + "_" + System.currentTimeMillis();

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        // get input data
        DataStreamSource<String> text = env.readTextFile(inputPath);
        SingleOutputStreamOperator<Tuple2<String, Long>> counts = text
                .<Tuple2<String, Long>>flatMap((line, out) -> {
                    StringTokenizer tokenizer = new StringTokenizer(line);
                    while (tokenizer.hasMoreTokens()) {
                        out.collect(new Tuple2<>(tokenizer.nextToken(), 1L));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> t) throws Exception {
                        LockSupport.parkNanos(SECONDS.toNanos(1));
                        return t.f0;
                    }
                })
                .sum(1);

        // emit result
        counts.writeAsCsv(outputPath)
              .setParallelism(1);
        // execute program
        long t = System.currentTimeMillis();
        env.execute("Streaming WordCount Example");
        System.out.println("Time=" + (System.currentTimeMillis() - t));
    }
}

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


import org.HdrHistogram.Histogram;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class OutputParser {

    static Histogram histogram = new Histogram(5);

    public static void main(String[] args) throws Exception {
        Files.list(Paths.get("/private/tmp/results"))
             .peek(System.out::println)
             .forEach(OutputParser::createHistogram);

    }

    private static void createHistogram(Path path){
        System.out.println("histogram for " + path);
        histogram.reset();
        try {
            Files.lines(path)
                 .mapToInt(l -> {
                     String ts = l.split(",")[1];
                     return Integer.parseInt(ts.trim());
                 })
                 .filter( i -> {
                     if (i < 0) {
                         System.out.println("Negative " + i);
                         return false;
                     } else if (i == 0){
                         System.out.println("Zero");
                     }
                     return true;
                 })
                 .forEach(latency -> histogram.recordValue(latency));
            PrintStream printStream = new PrintStream(new FileOutputStream(path.toString() + ".histogram"));
            histogram.outputPercentileDistribution(printStream, 1.0);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LongSummaryStatistics;
import java.util.concurrent.Callable;

public class OutputParser {

    public static long minMaxDiff(String path) throws IOException {

        LongSummaryStatistics stats = Files.list(Paths.get(path))
                                                           .flatMap(f -> uncheckCall(() -> Files.lines(f)))
                                                           .mapToLong(l -> {
                                                               try {
                                                                   String ts = l.split(",")[3];
                                                                   if (ts.length() != 13) {
                                                                       throw new IllegalArgumentException();
                                                                   }
                                                                   return Long.valueOf(ts);
                                                               } catch (Exception ignored) {
                                                                   System.out.println("Malformed line: " + l);
                                                                   return Long.MIN_VALUE;
                                                               }
                                                           }).filter(l -> l > Long.MIN_VALUE)
                                                           .summaryStatistics();

        return stats.getMax() - stats.getMin();
    }

    public static void main(String [] args) throws IOException {
        System.out.println(minMaxDiff("jet-output"));
    }

    public static <T> T uncheckCall(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

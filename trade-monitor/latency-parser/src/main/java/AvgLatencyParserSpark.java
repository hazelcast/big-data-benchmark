/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LongSummaryStatistics;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AvgLatencyParserSpark {
    // parses line in the form: "(<key>,<count>,<latency>)"
    private static final Pattern PARSER = Pattern.compile("\\(T-[0-9]+,([0-9]+),([0-9]+)\\)");

    public static void run(String dirName) throws Exception {
        Path dir = Paths.get(dirName);
        if (!Files.isDirectory(dir)) {
            System.err.println(dir + " is not a directory");
            System.exit(1);
        }

        Files.list(dir)
             .sorted(Comparator.<Path>naturalOrder().reversed())
             .filter(subdir -> {
                 if (listFilteredFiles(subdir).count() == 0) {
                     System.out.println("Directory for " + toLocalTime(extractTimestamp(subdir)) + " has no output files");
                     return false;
                 }
                 return true;
             })
             .limit(30)
             .flatMap(AvgLatencyParserSpark::listFilteredFiles)
             .filter(file -> file.getFileName().toString().startsWith("part-"))
             .flatMap(file -> {
                 try {
                     long timestamp = extractTimestamp(file.getParent());
                     return Files.lines(file)
                                 .map(line -> {
                                     Matcher m = PARSER.matcher(line);
                                     m.find();
                                     return new long[] {timestamp, Long.parseLong(m.group(1)), Long.parseLong(m.group(2))};
                                 });
                 } catch (IOException e) {
                     throw new RuntimeException(e);
                 }
             })
             .collect(Collectors.groupingBy((long[] line) -> line[0], TreeMap::new, pairing(
                     Collectors.summarizingLong(line -> line[2]),
                     Collectors.summingLong(line -> line[1]),
                     SimpleEntry::new)))
             .forEach((key, value) -> {
                 LongSummaryStatistics stats = value.getKey();
                 System.out.println(String.format("%-8s avg=%4d nkeys=%,6d ntrades=%,9d min=%4d max=%4d",
                         toLocalTime(key), (long) stats.getAverage(), stats.getCount(), value.getValue(), stats.getMin(), stats.getMax()));
             });
    }

    public static long extractTimestamp(Path parent) {
        String parentDirName = parent.getFileName().toString();
        return Long.parseLong(parentDirName.substring(2));
    }

    private static Stream<Path> listFilteredFiles(Path dir) {
        try {
            return Files.list(dir)
                        .filter(file -> file.getFileName().toString().startsWith("part-"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static LocalTime toLocalTime(long startTime) {
        return Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault()).toLocalTime();
    }

    /**
     * Creates a collector from two other collectors.
     * Taken from https://stackoverflow.com/a/30211021/952135
     */
    private static <T, A1, A2, R1, R2, R> Collector<T, ?, R> pairing(Collector<T, A1, R1> c1,
                                                                     Collector<T, A2, R2> c2, BiFunction<R1, R2, R> finisher) {
        EnumSet<Characteristics> c = EnumSet.noneOf(Characteristics.class);
        c.addAll(c1.characteristics());
        c.retainAll(c2.characteristics());
        c.remove(Characteristics.IDENTITY_FINISH);
        return Collector.of(() -> new Object[]{c1.supplier().get(), c2.supplier().get()},
                (acc, v) -> {
                    c1.accumulator().accept((A1) acc[0], v);
                    c2.accumulator().accept((A2) acc[1], v);
                },
                (acc1, acc2) -> {
                    acc1[0] = c1.combiner().apply((A1) acc1[0], (A1) acc2[0]);
                    acc1[1] = c2.combiner().apply((A2) acc1[1], (A2) acc2[1]);
                    return acc1;
                },
                acc -> {
                    R1 r1 = c1.finisher().apply((A1) acc[0]);
                    R2 r2 = c2.finisher().apply((A2) acc[1]);
                    return finisher.apply(r1, r2);
                }, c.toArray(new Characteristics[c.size()]));
    }
}

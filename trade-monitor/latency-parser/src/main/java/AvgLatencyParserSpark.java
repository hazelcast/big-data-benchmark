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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;

class AvgLatencyParserSpark {


    private static final String[] benchmarks = {"500K-3K", "500K-50K", "2M-3K", "2M-50K", "4M-3K", "4M-50K"};

//    public static void run(String dirName) throws Exception {
//        Path dir = Paths.get(dirName);
//        if (!Files.isDirectory(dir)) {
//            System.err.println(dir + " is not a directory");
//            System.exit(1);
//        }
//
//        Files.list(dir)
//             .sorted(Comparator.<Path>naturalOrder().reversed())
//             .filter(subdir -> {
//                 if (listFilteredFiles(subdir).count() == 0) {
//                     System.out.println("Directory for " + toLocalTime(extractTimestamp(subdir)) + " has no output files");
//                     return false;
//                 }
//                 return true;
//             })
////             .limit(30)
//             .flatMap(AvgLatencyParserSpark::listFilteredFiles)
//             .filter(file -> file.getFileName().toString().startsWith("part-"))
//             .flatMap(file -> {
//                 try {
//                     long timestamp = extractTimestamp(file.getParent());
//                     return Files.lines(file)
//                                 .map(line -> {
//                                     Matcher m = PARSER.matcher(line);
//                                     m.find();
//                                     return new long[]{timestamp, Long.parseLong(m.group(1)), Long.parseLong(m.group(2))};
//                                 });
//                 } catch (IOException e) {
//                     throw new RuntimeException(e);
//                 }
//             })
//             .collect(Collectors.groupingBy((long[] line) -> line[0], TreeMap::new, pairing(
//                     Collectors.summarizingLong(line -> line[2]),
//                     Collectors.summingLong(line -> line[1]),
//                     SimpleEntry::new)))
//             .forEach((key, value) -> {
//                 LongSummaryStatistics stats = value.getKey();
//                 System.out.println(String.format("%-8s avg=%4d nkeys=%,6d ntrades=%,9d min=%4d max=%4d",
//                         toLocalTime(key), (long) stats.getAverage(), stats.getCount(), value.getValue(), stats.getMin(), stats.getMax()));
//             });
//    }

    public static void main(String[] args) throws Exception {
        parseSpark(Paths.get(args[0]));
    }

    static void parseSpark(Path runDirectory) throws IOException {
        System.out.println("###### Parsing Spark Benchmarks\n\n");
        for (String benchmark : benchmarks) {
            System.out.println("#### Benchmark: [" + benchmark + "]\n");
            parse(runDirectory, benchmark);
            System.out.println("\n-----------------------------------------------\n\n");
        }
    }

    private static void parse(Path runDirectory, String benchmark) throws IOException {
        Stream<Path> subDirs = Files.list(runDirectory);
        List<Map.Entry<Long, LongSummaryStatistics>> list =
                subDirs.flatMap(ip -> uncheckCall(() -> Files.list(ip)))
                       .filter(benchmarkPath -> benchmarkPath.getFileName().toString().equals("spark-" + benchmark))
                       .flatMap(benchmarkPath -> uncheckCall(() -> Files.list(benchmarkPath)))
                       .map(timedPath -> entry(timedPath.getFileName().toString(), findFile(timedPath.toFile())))
                       .collect(Collectors.groupingBy(Map.Entry::getKey))
                       .entrySet().stream()
                       .map(timedEntry -> {
                           LongSummaryStatistics statistics = timedEntry
                                   .getValue()
                                   .stream()
                                   .filter(e -> e.getValue() != null)
                                   .flatMap(e -> uncheckCall(() -> Files.lines(e.getValue().toPath())))
                                   .map(line -> line.split(","))
                                   .filter(split -> {
                                       if (split.length != 3) {
                                           System.err.println("incorrect line: " + Arrays.toString(split));
                                       }
                                       return split.length == 3;
                                   })
                                   .mapToLong(split -> Long.parseLong(split[2].substring(0, split[2].length() - 1)))
                                   .summaryStatistics();
                           long timestamp = extractTimestamp(timedEntry.getKey());
                           return entry(timestamp, statistics);
                       })
                       .collect(Collectors.toList());

        subDirs.close();

        OptionalLong min = list.stream().mapToLong(e -> e.getValue().getMin()).min();
        OptionalLong max = list.stream().mapToLong(e -> e.getValue().getMax()).max();
        OptionalDouble average = list.stream().mapToDouble(e -> e.getValue().getAverage()).average();

        System.out.println(String.format("Avg=%4d, min=%4d, max=%4d", (long) average.orElse(-1),
                min.orElse(-1), max.orElse(-1)));

        list.stream()
            .sorted(comparing(Map.Entry<Long, LongSummaryStatistics>::getKey).reversed())
            .forEach(e -> System.out.println(String.format("%-19s avg=%4d cnt=%5d min=%4d max=%4d",
                    toLocalTime(e.getKey()), (long) e.getValue().getAverage(), e.getValue().getCount(),
                    e.getValue().getMin(), e.getValue().getMax())));
    }

    private static long extractTimestamp(String directoryName) {
        return Long.parseLong(directoryName.substring(2));
    }

    private static File findFile(File timedFile) {
        if (!timedFile.isDirectory()) {
            return null;
        }
        try {

            File[] files = timedFile.listFiles();
            if (files == null) {
                return null;
            }
            for (File file : files) {
                if (file.getName().startsWith("part")) {
                    return file;
                }
                File subFile = findFile(file);
                if (subFile != null) {
                    return subFile;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    private static <T> T uncheckCall(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

//    private static Stream<Path> listFilteredFiles(Path dir) {
//        try {
//            return Files.list(dir)
//                        .filter(file -> file.getFileName().toString().startsWith("part-"));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    private static LocalTime toLocalTime(long startTime) {
        return Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault()).toLocalTime();
    }

//    /**
//     * Creates a collector from two other collectors.
//     * Taken from https://stackoverflow.com/a/30211021/952135
//     */
//    private static <T, A1, A2, R1, R2, R> Collector<T, ?, R> pairing(Collector<T, A1, R1> c1,
//                                                                     Collector<T, A2, R2> c2, BiFunction<R1, R2, R> finisher) {
//        EnumSet<Characteristics> c = EnumSet.noneOf(Characteristics.class);
//        c.addAll(c1.characteristics());
//        c.retainAll(c2.characteristics());
//        c.remove(Characteristics.IDENTITY_FINISH);
//        return Collector.of(() -> new Object[]{c1.supplier().get(), c2.supplier().get()},
//                (acc, v) -> {
//                    c1.accumulator().accept((A1) acc[0], v);
//                    c2.accumulator().accept((A2) acc[1], v);
//                },
//                (acc1, acc2) -> {
//                    acc1[0] = c1.combiner().apply((A1) acc1[0], (A1) acc2[0]);
//                    acc1[1] = c2.combiner().apply((A2) acc1[1], (A2) acc2[1]);
//                    return acc1;
//                },
//                acc -> {
//                    R1 r1 = c1.finisher().apply((A1) acc[0]);
//                    R2 r2 = c2.finisher().apply((A2) acc[1]);
//                    return finisher.apply(r1, r2);
//                }, c.toArray(new Characteristics[c.size()]));
//    }
}

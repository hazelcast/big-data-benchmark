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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingLong;

class AvgLatencyParserFlink {

    private static final String[] benchmarks = {"500K-3K", "500K-50K", "2M-3K", "2M-50K", "4M-3K", "4M-50K"};

    public static void main(String[] args) throws Exception {
        parseFlink(Paths.get(args[0]));
    }

    static void parseFlink(Path runDirectory) throws Exception {
        System.out.println("###### Parsing Flink Benchmarks\n\n");
        for (String benchmark : benchmarks) {
            System.out.println("#### Benchmark: [" + benchmark + "]\n");
            parse(runDirectory.toString() + "/out-" + benchmark);
            System.out.println("\n-----------------------------------------------\n\n");
        }
    }

    private static void parse(String file) throws Exception {
        Stream<String> lines = Files.lines(Paths.get(file));

        Map<Long, LongSummaryStatistics> statisticsMap =
                lines.parallel()
                     .map(line -> line.split(","))
                     .filter(line -> {
                         if (line.length != 5) {
                             System.err.println("incorrect line: " + Arrays.toString(line));
                         }
                         return line.length == 5;
                     })
                     .collect(groupingBy(line -> parseTime(line[0]), summarizingLong(line -> Long.parseLong(line[4]))));

        lines.close();

        OptionalLong min = statisticsMap.values().stream().mapToLong(LongSummaryStatistics::getMin).min();
        OptionalLong max = statisticsMap.values().stream().mapToLong(LongSummaryStatistics::getMax).max();
        OptionalDouble average = statisticsMap.values().stream().mapToDouble(LongSummaryStatistics::getAverage).average();

        System.out.println(String.format("Avg=%4d, min=%4d, max=%4d", (long) average.orElse(-1),
                min.orElse(-1), max.orElse(-1)));

        statisticsMap
                .entrySet().stream()
                .sorted(comparing(Entry<Long, LongSummaryStatistics>::getKey).reversed())
                .forEach(e -> System.out.println(String.format("%-19s avg=%4d cnt=%5d min=%4d max=%4d",
                        toLocalDateTime(e.getKey()), (long) e.getValue().getAverage(), e.getValue().getCount(),
                        e.getValue().getMin(), e.getValue().getMax())));

    }

    private static LocalDateTime toLocalDateTime(long startTime) {
        return Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    private static long parseTime(String s) {
        return LocalTime.parse(s).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}

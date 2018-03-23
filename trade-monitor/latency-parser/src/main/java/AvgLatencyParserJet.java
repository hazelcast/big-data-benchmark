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
import java.util.LongSummaryStatistics;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

public class AvgLatencyParserJet {
    public static void run(String fileName) throws Exception {
        Path file = Paths.get(fileName);
        if (!Files.isRegularFile(file)) {
            System.err.println(file + " is not a file");
            System.exit(1);
        }

        Files.lines(file)
             .map(line -> line.split(","))
             .collect(Collectors.groupingBy(line -> parseTime(line[0]), Collectors.summarizingLong(line -> Long.parseLong(line[4]))))
             .entrySet().stream()
             .sorted(comparing(Entry<Long, LongSummaryStatistics>::getKey).reversed())
             .limit(30)
             .forEach(e -> System.out.println(String.format("%-19s avg=%4d cnt=%5d min=%4d max=%4d",
                     toLocalDateTime(e.getKey()), (long) e.getValue().getAverage(), e.getValue().getCount(), e.getValue().getMin(), e.getValue().getMax())));
    }

    private static LocalDateTime toLocalDateTime(long startTime) {
        return Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    private static long parseTime(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return LocalTime.parse(s).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }
}

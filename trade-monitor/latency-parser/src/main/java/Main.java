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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            InputStream stream = Main.class.getResourceAsStream("usage.txt");
            copy(stream, System.err);
            System.exit(1);
        }

        switch (args[0]) {
            case "jet":
            case "flink":
                AvgLatencyParserJet.run(args[1]);
                break;
            case "spark":
                AvgLatencyParserSpark.run(args[1]);
                break;
            default:
                System.err.println("Unknown first argument: " + args[0]);
                System.exit(1);
        }
    }

    private static void copy(InputStream stream, PrintStream err) throws IOException {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(stream))) {
            for (String s; (s = r.readLine()) != null; ) {
                err.println(s);
            }
        }
    }
}

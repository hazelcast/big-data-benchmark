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

package com.hazelcast.benchmark.generator.map;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.stream.IStreamMap;

import java.io.IOException;

public class WordGenerator {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: word-generator <name> <num distinct> <num words>");
            return;
        }
        try {
            JetInstance client = Jet.newJetClient();
            IStreamMap<Long, String> map = client.getMap(args[0]);
            long distinctWords = Long.parseLong(args[1]);
            long numWords = Long.parseLong(args[2]);
            StringBuilder builder = new StringBuilder();
            for (long i = 0; i < numWords; i++) {
                builder.append(i % distinctWords).append(" ");
                if (i % 20 == 0) {
                    map.put(i, builder.toString());
                    builder = new StringBuilder();
                }
            }
            System.out.println(map.size());
        } finally {
            Jet.shutdownAll();
        }
    }
}

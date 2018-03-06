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

package com.hazelcast.jet.benchmark.wordcount;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.Map;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.HdfsProcessors.readHdfsP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;

public class HdfsToMap {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: hdfs-to-map <name> <input path> <parallelism>");
            return;
        }

        String name = args[0];
        String inputPath = args[1];
        int parallelism = Integer.parseInt(args[2]);

        JetInstance client = Jet.newJetClient();
        IMap<Long, String> map = client.getMap(name);
        map.clear();

        try {
            long begin = System.currentTimeMillis();
            fillMap(client, name, inputPath, parallelism);
            long elapsed = System.currentTimeMillis() - begin;
            System.out.println("Time=" + elapsed);
        } finally {
            client.shutdown();
        }
    }

    private static void fillMap(JetInstance client, String name, String inputPath, int parallelism) throws Exception {
        DAG dag = new DAG();
        JobConf conf = new JobConf();
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(conf, new Path(inputPath));


        Vertex reader = dag.newVertex("reader", readHdfsP(conf, Util::entry));
        Vertex mapper = dag.newVertex("mapper",
                mapP((Map.Entry<LongWritable, Text> e) -> entry(e.getKey().get(), e.getValue().toString())));
        Vertex writer = dag.newVertex("writer", writeMapP(name));

        reader.localParallelism(parallelism);
        mapper.localParallelism(parallelism);
        writer.localParallelism(parallelism);

        dag.edge(between(reader, mapper));
        dag.edge(between(mapper, writer));


        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(HdfsToMap.class);

        client.newJob(dag, jobConfig).join();
    }
}

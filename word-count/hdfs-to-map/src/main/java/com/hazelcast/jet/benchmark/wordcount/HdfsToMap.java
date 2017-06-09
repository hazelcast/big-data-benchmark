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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.stream.IStreamMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.Map;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.processor.HdfsProcessors.readHdfs;
import static com.hazelcast.jet.processor.Processors.map;
import static com.hazelcast.jet.processor.Sinks.writeMap;

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
        IStreamMap<Long, String> map = client.getMap(name);
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


        Vertex reader = dag.newVertex("reader", readHdfs(conf));
        Vertex mapper = dag.newVertex("mapper",
                map((Map.Entry<LongWritable, Text> e) -> entry(e.getKey().get(), e.getValue().toString())));
        Vertex writer = dag.newVertex("writer", writeMap(name));

        reader.localParallelism(parallelism);
        mapper.localParallelism(parallelism);
        writer.localParallelism(parallelism);

        dag.edge(between(reader, mapper));
        dag.edge(between(mapper, writer));


        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(HdfsToMap.class);

        client.newJob(dag, jobConfig).execute().get();
    }
}

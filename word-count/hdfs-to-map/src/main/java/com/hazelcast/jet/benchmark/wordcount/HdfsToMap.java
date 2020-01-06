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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import static com.hazelcast.jet.Util.entry;
import static java.lang.Integer.parseInt;

public class HdfsToMap {

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: hdfs-to-map <name> <hdfsUri> <input path>");
            return;
        }

        String name = args[0];
        String hdfsUri = args[1];
        String inputPath = args[2];
        int parallelism = parseInt(args[3]);

        JetInstance client = Jet.bootstrappedInstance();
        IMap<Long, String> map = client.getMap(name);
        map.clear();

        try {
            long begin = System.currentTimeMillis();
            fillMap(client, name, hdfsUri, inputPath, parallelism);
            long elapsed = System.currentTimeMillis() - begin;
            System.out.println("Time=" + elapsed);
        } finally {
            client.shutdown();
        }
    }

    private static void fillMap(JetInstance client, String name, String hdfsUri, String inputPath, int parallelism) {
        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(conf, new Path(inputPath));

        Pipeline p = Pipeline.create();

        p.readFrom(HadoopSources.<LongWritable, Text>inputFormat(conf)).setLocalParallelism(parallelism)
         .map(e -> entry(e.getKey().get(), e.getValue().toString())).setLocalParallelism(parallelism)
         .writeTo(Sinks.map(name)).setLocalParallelism(parallelism);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(HdfsToMap.class);

        client.newJob(p, jobConfig).join();
    }
}

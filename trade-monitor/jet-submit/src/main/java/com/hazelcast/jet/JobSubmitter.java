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

package com.hazelcast.jet;

import com.hazelcast.jet.config.JobConfig;

import java.lang.reflect.Method;
import java.util.jar.JarFile;

public class JobSubmitter
{
    private static String jar;

    public static void main(String[] args) throws Exception {
        int argLength = 1;
        if (args.length < argLength) {
            System.err.println("Usage:");
            System.err.println("  JobSubmitter <JAR>");
            System.exit(1);
        }

        jar = args[0];

        String mainClass = new JarFile(jar).getManifest().getMainAttributes().getValue("Main-Class");
        String[] jobArgs = new String[args.length - argLength];
        System.arraycopy(args, argLength, jobArgs, 0, args.length - argLength);

        ClassLoader classLoader = JobSubmitter.class.getClassLoader();
        Class<?> clazz = classLoader.loadClass(mainClass);
        Method main = clazz.getDeclaredMethod("main", String[].class);
        main.invoke(null, new Object[] { jobArgs });
    }

    public static Job newJob(JetInstance instance, DAG dag, JobConfig config) {
        config.addJar(jar);
        return instance.newJob(dag, config);
    }

    public static Job newJob(JetInstance instance, DAG dag) {
        return newJob(instance, dag, new JobConfig());
    }
}

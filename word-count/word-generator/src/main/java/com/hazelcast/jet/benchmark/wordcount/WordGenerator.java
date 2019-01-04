package com.hazelcast.jet.benchmark.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;


public class WordGenerator {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Usage: word-generator <path> <num distinct> <num words>");
            return;
        }
        String hdfsUri = args[0];
        String path = args[1];
        long total = Long.parseLong(args[2]);
        long distinct = Long.parseLong(args[3]);

        URI uri = URI.create(hdfsUri);
        String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean(disableCacheName, true);
        try (FileSystem fs = FileSystem.get(uri, conf)) {
            Path p = new Path(path);
            try (OutputStreamWriter stream = new OutputStreamWriter(fs.create(p))) {
                writeToFile(stream, distinct, total);
            }
        }
    }

    private static void writeToFile(OutputStreamWriter stream, long distinctWords, long numWords) throws IOException {
        for (long i = 0; i < numWords; i++) {
            stream.write(i % distinctWords + "");
            if (i % 20 == 0) {
                stream.write("\n");
            } else {
                stream.write(" ");
            }
        }
        stream.write("\n");
    }

}

package com.hazelcast.benchmark.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;


public class WordGenerator {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: word-generator <path> <num distinct> <num words>");
            return;
        }
        FileSystem fs = FileSystem.get(new Configuration());
        DataOutputStream hdfsFile = fs.create(new Path(args[0]));
        try (OutputStreamWriter stream = new OutputStreamWriter(hdfsFile)) {
            writeToFile(stream, Long.parseLong(args[1]), Long.parseLong(args[2]));
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

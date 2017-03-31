package com.hazelcast.jet.benchmark.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.StringTokenizer;

public class SparkWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaRDD<String> words = textFile.flatMap(LineIterator::new);
        JavaPairRDD<String, Long> pairs =
                words.mapToPair((PairFunction<String, String, Long>) s -> new Tuple2<>(s, 1L));
        JavaPairRDD<String, Long> counts =
                pairs.reduceByKey((Function2<Long, Long, Long>) (a, b) -> a + b);

        System.out.println("Starting task..");
        long t = System.currentTimeMillis();
        counts.saveAsTextFile(args[1] + "_" + t);
        System.out.println("Time=" + (System.currentTimeMillis() - t));

    }

    private static class LineIterator implements Iterator<String> {

        private final StringTokenizer tokenizer;

        LineIterator(String line) {
            this.tokenizer = new StringTokenizer(line);
        }

        @Override
        public boolean hasNext() {
            return tokenizer.hasMoreTokens();
        }

        @Override
        public String next() {
            return tokenizer.nextToken();
        }
    }
}

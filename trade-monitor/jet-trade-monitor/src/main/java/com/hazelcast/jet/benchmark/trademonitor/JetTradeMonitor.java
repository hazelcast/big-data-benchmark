package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.KafkaSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.server.JetBootstrap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.lang.System.currentTimeMillis;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            System.err.println("Usage:");
            System.err.println("  " + JetTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs> <snapshotIntervalMs> <snapshotMode> <outputPath>");
            System.err.println();
            System.err.println("<snapshotMode> - \"exactly-once\" or \"at-least-once\"");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        String offsetReset = args[2];
        int lagMs = Integer.parseInt(args[3]);
        int windowSize = Integer.parseInt(args[4]);
        int slideBy = Integer.parseInt(args[5]);
        int snapshotInterval = Integer.parseInt(args[6]);
        ProcessingGuarantee guarantee = ProcessingGuarantee.valueOf(args[7].toUpperCase().replace('-', '_'));
        String outputPath = args[8];

        Properties kafkaProps = getKafkaProperties(brokerUri, offsetReset);

        Pipeline p = Pipeline.create();
        p.drawFrom(KafkaSources.kafka(kafkaProps, (ConsumerRecord<Object, Trade> record) -> record.value(), topic))
         .addTimestamps(Trade::getTime, lagMs)
         .groupingKey(Trade::getTicker)
         .window(sliding(windowSize, slideBy))
         .aggregate(counting(), (start, end, key, value) -> {
             long timeMs = currentTimeMillis();
             long latencyMs = timeMs - end;
             return Instant.ofEpochMilli(end).atZone(ZoneId.systemDefault()).toLocalTime().toString()
                     + "," + key
                     + "," + value
                     + "," + timeMs
                     + "," + (latencyMs - lagMs);
         })
         .drainTo(Sinks.files(outputPath));

//        Jet.newJetInstance();

        JetInstance jet = JetBootstrap.getInstance();
        System.out.println("Executing job..");
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(snapshotInterval);
        config.setProcessingGuarantee(guarantee);
        Future<Void> future = jet.newJob(p, config).getFuture();

        System.in.read();

        System.out.println("Cancelling job...");
        future.cancel(true);
        Thread.sleep(1000);
        jet.shutdown();
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        //props.setProperty("metadata.max.age.ms", "5000");
        return props;
    }
}

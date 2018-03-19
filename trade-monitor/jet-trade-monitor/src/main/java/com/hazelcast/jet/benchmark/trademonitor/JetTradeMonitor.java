package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer.MessageType;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.server.JetBootstrap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer.MessageType.BYTE;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.lang.System.currentTimeMillis;

public class JetTradeMonitor {

    public static void main(String[] args) throws Exception {
        System.out.println("Arguments: " + Arrays.toString(args));
        if (args.length != 13) {
            System.err.println("Usage:");
            System.err.println("  " + JetTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs>" +
                    " <snapshotIntervalMs> <processingGuarantee> <outputPath> <latencyType> <kafkaParallelism>" +
                    " <sinkParallelism> <messageType>");
            System.err.println();
            System.err.println("<processingGuarantee> - \"none\" or \"exactly-once\" or \"at-least-once\"");
            System.err.println("<latencyType> - \"internal-latency\" or \"external-latency\"");
            System.err.println("<messageType> - byte|object");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = args[0];
        String topic = args[1];
        String offsetReset = args[2];
        int lagMs = Integer.parseInt(args[3].replace("_", ""));
        int windowSize = Integer.parseInt(args[4].replace("_", ""));
        int slideBy = Integer.parseInt(args[5].replace("_", ""));
        int snapshotInterval = Integer.parseInt(args[6].replace("_", ""));
        ProcessingGuarantee guarantee = ProcessingGuarantee.valueOf(args[7].toUpperCase().replace('-', '_'));
        String outputPath = args[8];
        LatencyType latencyType = LatencyType.valueOf(args[9].toUpperCase().replace('-', '_'));
        int kafkaParallelism = Integer.parseInt(args[10]);
        int sinkParallelism = Integer.parseInt(args[11]);
        MessageType messageType = MessageType.valueOf(args[12].toUpperCase());

        Properties kafkaProps = getKafkaProperties(brokerUri, offsetReset, messageType);

        Pipeline p = Pipeline.create();
        StreamStage<Trade> sourceStage;
        if (messageType == BYTE) {
            sourceStage = p.drawFrom(KafkaSources.kafka(kafkaProps, (ConsumerRecord<Object, byte[]> record) -> record.value(), topic))
                                              .setLocalParallelism(kafkaParallelism)
                                              .mapUsingContext(ContextFactory.withCreateFn(jet -> new DeserializeContext()), JetTradeMonitor::deserialize);
        } else {
            sourceStage = p.drawFrom(KafkaSources.kafka(kafkaProps, (ConsumerRecord<Object, Trade> record) -> record.value(), topic))
                                              .setLocalParallelism(kafkaParallelism);
        }
        if (latencyType == LatencyType.INTERNAL_LATENCY) {
            sourceStage = sourceStage.addTimestamps();
        } else {
            sourceStage = sourceStage.addTimestamps(Trade::getTime, lagMs);
        }
        sourceStage = sourceStage.setLocalParallelism(kafkaParallelism);

        sourceStage
                .groupingKey(Trade::getTicker)
                .window(sliding(windowSize, slideBy))
                .aggregate(counting(), (start, end, key, value) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - end;
                    if (latencyType == LatencyType.EXTERNAL_LATENCY) {
                        latencyMs -= lagMs;
                    }
                    return Instant.ofEpochMilli(end).atZone(ZoneId.systemDefault()).toLocalTime().toString()
                            + "," + key
                            + "," + value
                            + "," + timeMs
                            + "," + (latencyMs);
                })
                .drainTo(Sinks.files(outputPath))
                .setLocalParallelism(sinkParallelism);

        // uncomment one of the following lines
//        JetInstance jet = Jet.newJetInstance(); // uncomment for local execution
        JetInstance jet = JetBootstrap.getInstance(); // uncomment for execution using jet-submit.sh

        System.out.println("Executing job..");
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(snapshotInterval);
        config.setProcessingGuarantee(guarantee);
        Job job = jet.newJob(p, config);

        System.in.read();

        System.out.println("Cancelling job...");
        job.cancel();
        Thread.sleep(1000);
        jet.shutdown();
    }

    private static Trade deserialize(DeserializeContext ctx, byte[] bytes) {
        ctx.is.setBuffer(bytes);
        try {
            String ticker = ctx.ois.readUTF();
            long time = ctx.ois.readLong();
            int price = ctx.ois.readInt();
            int quantity = ctx.ois.readInt();
            return new Trade(time, ticker, quantity, price);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset, MessageType messageType) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", (messageType == BYTE ? ByteArrayDeserializer.class : TradeDeserializer.class).getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        //props.setProperty("metadata.max.age.ms", "5000");
        return props;
    }

    private enum LatencyType {
        INTERNAL_LATENCY,
        EXTERNAL_LATENCY
    }

    private static class DeserializeContext {
        final ResettableByteArrayInputStream is = new ResettableByteArrayInputStream();
        final DataInputStream ois = new DataInputStream(is);
    }

    private static class ResettableByteArrayInputStream extends ByteArrayInputStream {

        ResettableByteArrayInputStream() {
            super(new byte[0]);
        }

        void setBuffer(byte[] data) {
            super.buf = data;
            super.pos = 0;
            super.mark = 0;
            super.count = data.length;
        }
    }
}

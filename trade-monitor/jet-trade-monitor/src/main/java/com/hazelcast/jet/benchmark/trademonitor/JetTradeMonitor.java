package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer.MessageType;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.server.JetBootstrap;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer.MessageType.BYTE;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JetTradeMonitor {

    private static final AggregateOperation1<Long, Histogram, String> latencyProfile = AggregateOperation
            .withCreate(() -> new Histogram(5))
            .<Long>andAccumulate(Histogram::recordValue)
            .andCombine(Histogram::add)
            .andExportFinish(histogram -> {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                PrintStream out = new PrintStream(bos);
                histogram.outputPercentileDistribution(out, 1.0);
                out.close();
                return bos.toString();
            });

    public static void main(String[] args) throws Exception {
        System.out.println("Arguments: " + Arrays.toString(args));
        if (args.length != 12) {
            System.err.println("Usage:");
            System.err.println("  " + JetTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs>" +
                    " <snapshotIntervalMs> <processingGuarantee> <outputPath> <latencyType> <kafkaParallelism>" +
                    " <sinkParallelism> <messageType>");
            System.err.println();
            System.err.println("<processingGuarantee> - \"none\" or \"exactly-once\" or \"at-least-once\"");
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
        int kafkaParallelism = Integer.parseInt(args[9]);
        int sinkParallelism = Integer.parseInt(args[10]);
        MessageType messageType = MessageType.valueOf(args[11].toUpperCase());

        Properties kafkaProps = getKafkaProperties(brokerUri, offsetReset, messageType);

        Pipeline p = Pipeline.create();
        StreamStage<Trade> sourceStage = (messageType == BYTE) 
                ? p.drawFrom(KafkaSources
                    .kafka(kafkaProps, (ConsumerRecord<Object, byte[]> record) -> record.value(), topic))
                    .withoutTimestamps()
                    .mapUsingContext(
                            ContextFactory.withCreateFn(jet -> new Deserializer()),
                            Deserializer::deserialize)
                : p.drawFrom(KafkaSources
                    .kafka(kafkaProps, (ConsumerRecord<Object, Trade> record) -> record.value(), topic))
                    .withoutTimestamps().setLocalParallelism(kafkaParallelism);
        sourceStage
                .addTimestamps(Trade::getTime, lagMs).setLocalParallelism(kafkaParallelism)
                .groupingKey(Trade::getTicker)
                .window(sliding(windowSize, slideBy))
                .aggregate(counting())
                .map(kwr -> currentTimeMillis() - kwr.end() - lagMs)
                .window(tumbling(Long.MAX_VALUE - 1).setEarlyResultsPeriod(SECONDS.toMillis(30)))
                .aggregate(latencyProfile).setLocalParallelism(1)
                .map(WindowResult::result)
                .drainTo(Sinks.files(outputPath)).setLocalParallelism(sinkParallelism);

        // uncomment one of the following lines
//        JetInstance jet = Jet.newJetInstance(); // uncomment for local execution
        JetInstance jet = JetBootstrap.getInstance(); // uncomment for execution using jet-submit.sh

        System.out.println("Executing job..");
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(snapshotInterval);
        config.setProcessingGuarantee(guarantee);
        jet.newJob(p, config).join();
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

    private static class Deserializer {
        final ResettableByteArrayInputStream is = new ResettableByteArrayInputStream();
        final DataInputStream ois = new DataInputStream(is);

        Trade deserialize(byte[] bytes) {
            is.setBuffer(bytes);
            try {
                String ticker = ois.readUTF();
                long time = ois.readLong();
                int price = ois.readInt();
                int quantity = ois.readInt();
                return new Trade(time, ticker, quantity, price);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
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

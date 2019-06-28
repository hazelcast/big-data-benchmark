package com.hazelcast.jet.benchmark.trademonitor;

import com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer.MessageType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;
import java.util.Properties;

import static com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer.MessageType.BYTE;
import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class KafkaStreamsTradeMonitor {

    public static void main(String[] args) {
        System.out.println("Arguments: " + Arrays.toString(args));
        if (args.length != 10) {
            System.err.println("Usage:");
            System.err.println("  " + KafkaStreamsTradeMonitor.class.getSimpleName() +
                    " <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs>" +
                    " <snapshotIntervalMs> <outputPath> <kafkaParallelism>" +
                    " <messageType>");
            System.err.println();
            System.err.println("<messageType> - byte|object");
            System.exit(1);
        }
        System.setProperty("hazelcast.logging.type", "log4j");
        Logger logger = Logger.getLogger(KafkaStreamsTradeMonitor.class);
        String brokerUri = args[0];
        String topic = args[1];
        String offsetReset = args[2];
        int lagMs = Integer.parseInt(args[3].replace("_", ""));
        int windowSize = Integer.parseInt(args[4].replace("_", ""));
        int slideBy = Integer.parseInt(args[5].replace("_", ""));
        int snapshotInterval = Integer.parseInt(args[6].replace("_", ""));
        String outputPath = args[7];
        int kafkaParallelism = Integer.parseInt(args[8]);
        MessageType messageType = MessageType.valueOf(args[9].toUpperCase());
        logger.info(String.format("" +
                        "Starting Jet Trade Monitor with the following parameters:%n" +
                        "Kafka broker URI            %s%n" +
                        "Kafka topic                 %s%n" +
                        "Auto-reset message offset?  %s%n" +
                        "Allowed lag                 %s ms%n" +
                        "Sliding window size         %s ms%n" +
                        "Slide by                    %s ms%n" +
                        "Snapshot interval           %s ms%n" +
                        "Output path                 %s%n" +
                        "Kafka thread parallelism    %s%n" +
                        "Message type                %s%n",
                brokerUri, topic, offsetReset, lagMs, windowSize, slideBy, snapshotInterval,
                outputPath, kafkaParallelism, messageType
        ));

        Properties streamsProperties = getKafkaStreamsProperties(brokerUri, offsetReset, messageType, kafkaParallelism);

        Serde<Trade> tradeSerde = Serdes.serdeFrom(new TradeSerializer(), new TradeDeserializer());
//        final StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, Trade> stream = builder.stream(topic, Consumed.with(Serdes.String(), tradeSerde));
//        stream
//                .selectKey((key1, value1) -> value1.getTicker())
//                .mapValues((readOnlyKey, value) -> System.currentTimeMillis() - value.getTime())
//                .filter((key, value) -> value > 0)
//                .print(Printed.toSysOut());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, Trade> stream = builder.stream(topic, Consumed.with(Serdes.Long(), tradeSerde));
        stream
                .selectKey((key1, value1) -> value1.getTicker())
                .groupByKey()
                .windowedBy(TimeWindows.of(ofMillis(windowSize))
                                       .advanceBy(ofMillis(slideBy))
                                       .grace(ofMillis(lagMs)))
                .count()
                .suppress(untilWindowCloses(BufferConfig.unbounded()))
                .mapValues((readOnlyKey, value) -> System.currentTimeMillis() - readOnlyKey.window().end() - lagMs)
                .filter((key, value) -> value > 0)
                .toStream()
                .print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (final Exception e) {
                // ignored
            }
        }));

    }

    private static Properties getKafkaStreamsProperties(String brokerUrl, String offsetReset, MessageType messageType, int kafkaParallelism) {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-trade-monitor");
        props.setProperty(StreamsConfig.CLIENT_ID_CONFIG, "kafka-streams-trade-monitor-client");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());
        props.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, String.valueOf(kafkaParallelism));
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, String.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                (messageType == BYTE ? ByteArrayDeserializer.class : TradeDeserializer.class).getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "32768");
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "100000000");
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");

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

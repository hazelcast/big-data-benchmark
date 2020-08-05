# Streaming Benchmark

The streaming benchmark is intended to measure the latency overhead
for a streaming system under different conditions such as message
rate and window size. It compares Hazelcast Jet, Apache Flink,
and Apache Spark Streaming.

## How to Run the Benchmarks

All benchmarks begin with these common steps:

1. Start Apache Kafka, see the [Kafka
   documentation](https://kafka.apache.org/documentation/)

2. Build the benchmarking code:

```bash
$ cd /path/to/big-data-benchmark
$ mvn clean package -pl trade-monitor/kafka-trade-producer -am
```

3. Start the program that produces events to Kafka:

```bash
$ cd trade-monitor/kafka-trade-producer
$ java -cp target/kafka-trade-producer-4.3-SNAPSHOT.jar \
com.hazelcast.jet.benchmark.trademonitor.KafkaTradeProducer \
localhost:9092 4 \
1_000_000 1024 OBJECT
```

The parameters are:

```
<Kafka broker URI> <num parallel producers> \
<trades per second> <num distinct keys> <messageType>
```

The producer will emit the given number of trade events per second,
and it will use `<num producers>` threads to do it. Every thread runs
its own instance of a Kafka Producer client and produces its share of
the requested events per second, using its distinct share of the
requested keyset size.


## Jet benchmark

1. Download and unzip the Hazelcast Jet distribution package, see the
[Jet documentation](https://jet-start.sh/docs/operations/installation).

2. Build the Pipeline JAR and copy to the `lib` folder on all Jet nodes.
Here we copy it to the local Jet installation:

```bash
$ cd /path/to/big-data-benchmark
$ mvn clean package -pl trade-monitor/jet-trade-monitor -am
$ cp trade-monitor/jet-trade-monitor/target/jet-trade-monitor-4.3-SNAPSHOT.jar \
/path/to/hazelcast-jet/lib
```

3. Start a Jet node:

```bash
$ cd /path/to/hazelcast-jet
$ bin/jet-start
```

3. Submit the job

```bash
$ cd /path/to/hazelcast-jet
$ bin/jet -v submit lib/jet-trade-monitor-4.3-SNAPSHOT.jar \
localhost:9092 latest OBJECT \
4 5000 \
1000 100 \
AT_LEAST_ONCE 10000 \
20 240 benchmark-results/
```

The parameters are:

```
<Kafka broker URI> <message offset auto-reset> <message type>
<Kafka source local parallelism> <max event lag ms> \
<window size ms> <sliding step ms> \
<processing guarantee> <snapshot interval ms> \
<warmup seconds> <measurement seconds> <output path>
```

4. Retrieve the Results

The results will be where you specified when submitting the job, in our example
it is the `benchmark-results` folder.

To quickly plot the results, you can use `gnuplot` :

```
$ gnuplot -e "plot 'target/0','target/1'; pause -1"
```

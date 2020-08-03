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
$ mvn package -pl trade-monitor/kafka-trade-producer -am
```

3. Start the program that produces events to Kafka:

```bash
$ cd trade-monitor/kafka-trade-producer
$ java -cp target/kafka-trade-producer-4.3-SNAPSHOT.jar \
com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer \
localhost:9092 trades 16 1000 \
1024 OBJECT
```

The meaning of parameters:

```
...RealTimeTradeProducer.jar \
<bootstrap.servers> <topic> <num producers> <trades per second> \
<num distinct keys> <messageType>
```

## Jet benchmark

1. Download and unzip the Hazelcast Jet distribution package, see
   [Jet
   documentation](https://jet-start.sh/docs/operations/installation).

2. Build the Pipeline JAR and copy to the `lib` folder on all Jet nodes.
Here we copy it to the local Jet installation:

```bash
$ cd /path/to/big-data-benchmark
$ mvn package -pl trade-monitor/jet-trade-monitor -am
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
localhost:9092 trades latest 5000 1000 100 \
10000 AT_LEAST_ONCE results/ \
4 4 OBJECT
```

The parameters are:

```
<bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs> \
<snapshotIntervalMs> <processingGuarantee> <outputPath> \
<kafkaParallelism> <sinkParallelism> <messageType>
```

4. Cancel the job

After running the job for required time you can cancel the job by running
the following command:

```bash
bin/jet cancel JetTradeMonitor
```

4. Measure latency

The results can be found in the `results` folder, in files named 0, 1, 2 ...
up to a number depending on the `sinkParallelism` parameter.

The files contain latencies for individual windows. You can process them further,
e.g. to compute percentiles, maximum latency, plot histograms etc..

For example, to simply plot all results you can:

```
gnuplot -e "plot 'target/0','target/1'; pause -1"
```

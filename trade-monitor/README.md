# Streaming Benchmark

The streaming benchmark is intended to measure the latency overhead
for a streaming system under different conditions such as message 
rate and window size. It compares Hazelcast Jet, Apache Flink, 
and Apache Spark Streaming.

## How to Run the Benchmarks

Prerequisites for all benchmarks:

Apache Kafka cluster up and running
See [Kafka documentation](https://kafka.apache.org/documentation/)

1. Build the benchmark

Run the following command from the `trade-monitor` directory:
 
```bash
mvn package -pl trade-monitor/kafka-trade-producer -am
```

2. Start the kafka producer

```bash
cd kafka-trade-producer
java -cp target/kafka-trade-producer-4.3-SNAPSHOT.jar com.hazelcast.jet.benchmark.trademonitor.RealTimeTradeProducer localhost:9092 trades 16 1000 1024 OBJECT
```

The parameters are:

```
...RealTimeTradeProducer <bootstrap.servers> <topic> <num producers> <trades per second> <num distinct keys> <messageType>
```

## Jet benchmark

To run the Jet benchmark you need a Jet cluster up and running
See [Jet documentation](https://jet-start.sh/docs/operations/installation)

1. Copy job jar to the lib directory of all Jet nodes

```bash
cp big-data-benchmark/trade-monitor/jet-trade-monitor/target/jet-trade-monitor-4.3-SNAPSHOT.jar hazelcast-jet-4.3-SNAPSHOT/lib
```

2. Submit the job

```bash
bin/jet -v submit lib/jet-trade-monitor-4.3-SNAPSHOT.jar localhost:9092 trades latest 5000 1000 100 \
  10000 AT_LEAST_ONCE results/ 4 
  4 OBJECT
```

The parameters are:

```  
 <bootstrap.servers> <topic> <offset-reset> <maxLagMs> <windowSizeMs> <slideByMs>
 <snapshotIntervalMs> <processingGuarantee> <outputPath> <kafkaParallelism>
 <sinkParallelism> <messageType>
```

3. Cancel the job

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

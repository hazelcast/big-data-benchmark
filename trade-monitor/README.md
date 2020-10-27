# Streaming Benchmark

The streaming benchmark is intended to measure the latency for a
streaming system under different conditions such as message rate and
window size. There is code in this project for Hazelcast Jet, Apache
Flink and Apache Spark, but only the code for Hazelcast Jet is up to
date.

The benchmark consists of generating a stream of stock trade events,
sending them to a Kafka topic `trades`, and then performing sliding
window aggregation on them with a distributed stream processing engine.

# Steps to Reproduce the Benchmark Results

## Set Up EC2 Instances

1. Create these EC2 instances:

```text
    Name       Type
  ---------  ----------
  Producer   c5.4xlarge
  Kafka-1    i3.2xlarge
  Kafka-2    i3.2xlarge
  Kafka-3    i3.2xlarge
  Jet-1      c5.4xlarge
  Jet-2      c5.4xlarge
  Jet-3      c5.4xlarge
```

2. Set a tag on the `Jet-*` instances for automatic cluster discovery.
   For example, create a tag `JetClusterName = BigDataBenchmark`. You'll
   use this later in Jet configuration.

3. Open these inbound ports, CIDR 10.0.0.0/24:

```text
    Port   Purpose
    ----   ---------
    2181   ZooKeeper
    9020   Kafka
    5701   Hazelcast
```

We used a single EC2 Security Group with all these open ports, but if
you prefer to open the very minimum, `Producer` doesn't need any inbound
ports open, the `Kafka-*` machines need the Kafka port, `Kafka-1`
additionally needs the ZooKeeper port, and the `Jet-*` machines need the
Hazelcast port.

## Build and Upload the Benchmark Code

On local machine (assuming Java and Maven already installed), do these:

```bash
$ cd /path/to/big-data-benchmark/trade-monitor
$ mvn clean package
...
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  11.864 s
[INFO] Finished at: 2020-10-05T20:31:08+02:00
[INFO] ------------------------------------------------------------------------
$ cd kafka-trade-producer
$ scp kafka-trade-producer.properties target/kafka-trade-producer-1.0-SNAPSHOT.jar ec2-user@<producer-public-ip>:
$ cd ../jet-trade-monitor
$ scp jet-trade-monitor.properties target/jet-trade-monitor-1.0-SNAPSHOT.jar ec2-user@<producer-public-ip>:
```

## Install Java

To replicate the setup on many nodes, we recommend using a terminal
emulator that can broadcast your typing to several SSH sessions at once.

On all the EC2 instances, do this:

```bash
$ wget https://download.java.net/java/GA/jdk15/779bf45e88a44cbd9ea6621d33e33db1/36/GPL/openjdk-15_linux-x64_bin.tar.gz
...
Connecting to download.java.net (download.java.net)|104.70.188.49|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 195313679 (186M) [application/x-gzip]
Saving to: ‘openjdk-15_linux-x64_bin.tar.gz’
...
$ tar xvf openjdk-15_linux-x64_bin.tar.gz
```

This is OpenJDK 15, build number 36, which is the initial GA release of
JDK 15\. We'll refer to this Java installation explicitly in the
commands that start Kafka, Jet, and the Kafka Producer.

## Set Up Kafka

On the three `Kafka-*` instances, perform these steps:

### Mount the SSD Instance Store

```bash
$ sudo mkfs -t xfs /dev/nvme0n1
meta-data=/dev/nvme0n1           isize=512    agcount=4, agsize=115966797 blks
         =                       sectsz=512   attr=2, projid32bit=1
         =                       crc=1        finobt=1, sparse=0
data     =                       bsize=4096   blocks=463867187, imaxpct=5
         =                       sunit=0      swidth=0 blks
naming   =version 2              bsize=4096   ascii-ci=0 ftype=1
log      =internal log           bsize=4096   blocks=226497, version=2
         =                       sectsz=512   sunit=0 blks, lazy-count=1
realtime =none                   extsz=4096   blocks=0, rtextents=0
$ mkdir big
$ sudo mount /dev/nvme0n1 big/
$ sudo chown ec2-user:ec2-user big
```

### Install and Configure Kafka

```bash
$ wget http://mirror.cc.columbia.edu/pub/software/apache/kafka/2.6.0/kafka_2.13-2.6.0.tgz
...
HTTP request sent, awaiting response... 200 OK
Length: 65537909 (63M) [application/x-gzip]
Saving to: ‘kafka_2.13-2.6.0.tgz’
...
2020-10-05 11:35:17 (77.4 MB/s) - ‘kafka_2.13-2.6.0.tgz’ saved [65537909/65537909]

$ tar xvf kafka_2.13-2.6.0.tgz
kafka_2.13-2.6.0/
kafka_2.13-2.6.0/LICENSE
kafka_2.13-2.6.0/NOTICE
...
kafka_2.13-2.6.0/libs/kafka-streams-scala_2.13-2.6.0.jar
kafka_2.13-2.6.0/libs/kafka-streams-test-utils-2.6.0.jar
kafka_2.13-2.6.0/libs/kafka-streams-examples-2.6.0.jar
$ cd kafka_2.13-2.6.0
$ vi config/server.properties
```

Many of the following properties are already present in the config file.
Edit/add/comment them out as appropriate.:

```text
#broker.id=0
broker.id.generation.enable=true

listeners=PLAINTEXT://<local-private-ip>:9092

log.dirs=/home/ec2-user/big/kafka-logs

log.retention.hours=1000
log.retention.bytes=1073741824
log.retention.check.interval.ms=30000000

zookeeper.connect=<kafka-1-private-ip>:2181
```

For ZooKeeper, you need to set just one property:

```bash
$ vi config/zookeeper.properties
```
```bash
dataDir=/home/ec2-user/big/zookeeper
```

### Start ZooKeeper

Do this on `Kafka-1` only:

```bash
$ bin/zookeeper-server-start.sh config/zookeeper.properties
...
[2020-10-05 12:44:04,863] INFO Created server with tickTime 3000 minSessionTimeout 6000 maxSessionTimeout 60000 datadir /tmp/zookeeper/version-2 snapdir /tmp/zookeeper/version-2 (org.apache.zookeeper.server.ZooKeeperServer)
...
<Type Ctrl-Z to get the prompt back>
[1]+  Stopped                 bin/zookeeper-server-start.sh config/zookeeper.properties
$ bg
[1]+ bin/zookeeper-server-start.sh config/zookeeper.properties &
```

### Start the Kafka Cluster

Do this on all three `Kafka-*` instances:

```bash
$ JAVA_HOME="/home/ec2-user/jdk-15" \
KAFKA_HEAP_OPTS="-Xms4g -Xmx4g" \
KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseZGC -XX:+ExplicitGCInvokesConcurrent
-XX:MaxInlineLevel=15 -Djava.awt.headless=true" \
bin/kafka-server-start.sh config/server.properties
...
[2020-10-05 12:46:53,611] INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
...
```

For comparison, Kafka uses these setting by default:

```bash
KAFKA_HEAP_OPTS="-Xms1g -Xmx1g"
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20
-XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent
-XX:MaxInlineLevel=15 -Djava.awt.headless=true"
```

So our changes are these:

1. enabled ZGC instead of G1 GC
2. increased heap size: 1 GB -> 4 GB
3. removed G1-specific `MaxGCPauseMillis` and `InitiatingHeapOccupancyPercent`

### Create the `trades` Topic

Do this on `Kafka-1` only:

```bash
<Type Ctrl-Z to get the prompt back>
[2]+  Stopped                 JAVA_HOME="/home/ec2-user/jdk-15" KAFKA_HEAP_OPTS="-Xms4g -Xmx4g" KAFKA_JVM_PERFORMANCE_OPTS="-XX:MaxGCPauseMillis=5 -XX:MaxNewSize=100m -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true" bin/kafka-server-start.sh config/server.properties
$ bg
[2]+ JAVA_HOME="/home/ec2-user/jdk-15" KAFKA_HEAP_OPTS="-Xms4g -Xmx4g" KAFKA_JVM_PERFORMANCE_OPTS="-XX:MaxGCPauseMillis=5 -XX:MaxNewSize=100m -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true" bin/kafka-server-start.sh config/server.properties &
$ bin/kafka-topics.sh --bootstrap-server <kafka-1-private-ip>:9092 --create \
--topic trades --partitions 24 --replication-factor 3
<Kafka logging appears>
Created topic trades.
$ fg
```

## Set Up Hazelcast Jet

### Install Hazelcast Jet

Do this on all `Jet-*` instances as well as `Producer`:

```bash
$ wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.2/hazelcast-jet-4.2.tar.gz
...
HTTP request sent, awaiting response... 200 OK
Length: 106779502 (102M) [application/octet-stream]
Saving to: ‘hazelcast-jet-4.2.tar.gz’
...
$ tar xvf hazelcast-jet-4.2.tar.gz
hazelcast-jet-4.2/LICENSE
hazelcast-jet-4.2/NOTICE
hazelcast-jet-4.2/README.md
...
hazelcast-jet-4.2/opt/hazelcast-jet-s3-4.2.jar
hazelcast-jet-4.2/opt/hazelcast-jet-spring-4.2.jar
hazelcast-jet-4.2/release_notes.txt
$ mv hazelcast-jet-4.2/opt/hazelcast-jet-kafka-4.2.jar hazelcast-jet-4.2/lib/
$ vi ~/.bash_profile
```

```bash
export PATH=$PATH:/home/ec2-user/hazelcast-jet-4.2/bin
```

Log out and back in for the path to take effect.

### Configure and Run the Jet Cluster

For this step you need your AWS access key ID and secret access key.

On all `Jet-*` instances do this:

```bash
vi hazelcast-jet-4.2/config/hazelcast.yaml
```

Find and edit these sections:

```yaml
network:join:multicast:
    enabled: false

network:join:aws:
    enabled: true
    access-key: <ec2-access-key.id>
    secret-key: <ec2-access-key.secret>
    <delete iam-role>
    <delete region>
    <delete host-header>
    <delete security-group-name>
    tag-key: JetClusterName
    tag-value: BigDataBenchmark
    hz-port: 5701
```

```bash
vi hazelcast-jet-4.2/config/hazelcast-jet.yaml
```
Configure the cooperative thread count:
```yaml
hazelcast-jet:
  instance:
    cooperative-thread-count: 12
```

Start Jet:

```bash
$ JAVA_HOME=/home/ec2-user/jdk-15 \
JAVA_OPTS="-Xmx8g -XX:MaxGCPauseMillis=5 -XX:MaxNewSize=100m" \
jet-start
...
Starting Hazelcast Jet
...
Members {size:3, ver:3} [
	Member [10.0.0.96]:5701 - 65eddc54-cbe2-4908-b616-2d87c228a2ca this
	Member [10.0.0.247]:5701 - b751def9-ccc4-4427-8d05-56befca58ff0
	Member [10.0.0.168]:5701 - 2cf59cdc-3514-4cb3-8048-3829e549bd03
]
```

## Run the Benchmark

### Configure the Trade Event Producer

On the `Producer` instance, configure the parameters for the trade
event producer:

```bash
$ vi kafka-trade-producer.properties
```
```
kafka-broker-uri=<kafka-1-private-ip>:9092
num-parallel-producers=12
trades-per-second=6_000_000
num-distinct-keys=50_000
```

The producer program emits the given number of trade events per second
to the Kafka topic `trades`, and uses `num-parallel-producers` threads
to do so. Every thread runs its own instance of a Kafka Producer client
and produces its share of the requested events per second, using its
distinct share of the requested keyset size. The program queries Kafka
for the number of partitions the topic uses and distributes these
partitions equally among the producer threads.

The trade event timestamps are predetermined and don't depend on the
current time. Effectively, this program simulates a constant stream of
equally-spaced trade events. It guarantees it won't try to send an event
to Kafka before it has occurred, but there's no guarantee on how much
later it will manage to send it. If the requested throughput is too
high, the producer may be increasingly falling back behind real time.
The timestamps it emits will still be the same, but this delay in
sending the events contributes to the reported end-to-end latency. You
can track this in the program's output.

### Configure Hazelcast Jet Client and the Benchmark Job

Perform this step on the `Producer` instance.

You'll be using the `jet submit` command to run the benchmark job. It
needs the IP address of one of the Jet cluster nodes, so:

```bash
$ vi hazelcast-jet-4.2/config/hazelcast-client.yaml
```
```
hazelcast-jet-client:network:cluster-members:
    - <jet-1-private-ip>
```

Configure the benchmark parameters:

```bash
$ vi jet-trade-monitor.properties
```
```
broker-uri=<kafka-1-private-ip>:9092
offset-reset=latest
kafka-source-local-parallelism=8
window-size-millis=20_000
sliding-step-millis=20
processing-guarantee=none
warmup-seconds=40
measurement-seconds=360
output-path=/home/ec2-user/benchmark-results
```

The benchmarking job reads the trade events that the producer program
sent to Kafka. It performs sliding window aggregation on them (computes
trades-per-second for each ticker) and then records the end-to-end
latency: how much after the window's end timestamp was Jet able to emit
the first key-value pair of the window result. Jet guarantees it won't
emit a window result before having observed all the events up to its end
time.

Generally, the parallelism of the Kafka source should match the number
of partitions in the `trades` topic. In the configuration you specify
the _local_ parallelism of the Kafka source, to get the total
parallelism multiply it by the number of nodes in the Jet cluster. If
the total parallelism is less than the number of partitions, some source
threads will read from more than one partition. If it is higher, some
threads will remain idle.

### Run the Event Producer And the Benchmark Job

In one SSH session with `Producer`:

```bash
$ java -jar kafka-trade-producer-1.0-SNAPSHOT.jar
...
Kafka producer started with these settings:
    kafka-broker-uri=10.0.0.140:9092
    num-parallel-producers=12
    trades-per-second=5,000,000
    num-distinct-keys=50,000
    debug-mode=false
Topic trades has 24 partitions

 6: 372,848 events/second, 210 ms worst latency
 2: 377,305 events/second, 189 ms worst latency
 9: 389,485 events/second, 130 ms worst latency
 4: 357,037 events/second, 286 ms worst latency
 0: 391,679 events/second, 119 ms worst latency
 5: 361,419 events/second, 265 ms worst latency
 7: 374,384 events/second, 202 ms worst latency
 1: 318,415 events/second, 471 ms worst latency
11: 356,224 events/second, 290 ms worst latency
 3: 350,207 events/second, 319 ms worst latency
 8: 390,516 events/second, 125 ms worst latency
10: 352,510 events/second, 307 ms worst latency
10: 480,823 events/second, 19 ms worst latency
 1: 514,934 events/second, 24 ms worst latency
 5: 471,923 events/second, 7 ms worst latency
 7: 458,952 events/second, 16 ms worst latency
 0: 441,655 events/second, 30 ms worst latency
 8: 442,818 events/second, 10 ms worst latency
11: 477,109 events/second, 5 ms worst latency
 6: 460,488 events/second, 2 ms worst latency
 3: 483,127 events/second, 4 ms worst latency
 9: 443,850 events/second, 7 ms worst latency
 2: 456,047 events/second, 12 ms worst latency
 4: 476,310 events/second, 2 ms worst latency
...
```

In another SSH session with `Producer`:

```bash
$ jet submit -v jet-trade-monitor-1.0-SNAPSHOT.jar
...
Benchmarking job is now in progress, let it run until you see the message
"benchmarking is done" in the Jet server log,
and then stop it here with Ctrl-C. The result files are on the server.
```

On one of the `Jet-*` nodes, you'll see the job's DAG printed:

```text
Start executing job 'Trade Monitor Benchmark', execution 0512-7caa-4340-0001, execution graph in DOT format:
digraph DAG {
	"streamKafka(trades)" [localParallelism=8];
	"sliding-window" [localParallelism=12];
	"filter" [localParallelism=12];
	"map-stateful-global" [localParallelism=1];
	"map-stateful-global-2" [localParallelism=1];
	"filesSink(/home/ec2-user/benchmark-results/latency-profile)" [localParallelism=1];
	"fused(filter-2, map)" [localParallelism=12];
	"filesSink(/home/ec2-user/benchmark-results/latency-log)" [localParallelism=1];
	"streamKafka(trades)" -> "sliding-window" [label="distributed-partitioned", queueSize=1024];
	"sliding-window" -> "filter" [queueSize=1024];
	"filter" -> "map-stateful-global" [label="distributed-partitioned", queueSize=1024];
	"map-stateful-global" -> "fused(filter-2, map)" [taillabel=0, queueSize=1024];
	"map-stateful-global" -> "map-stateful-global-2" [label="distributed-partitioned", taillabel=1, queueSize=1024];
	"map-stateful-global-2" -> "filesSink(/home/ec2-user/benchmark-results/latency-profile)" [queueSize=1024];
	"fused(filter-2, map)" -> "filesSink(/home/ec2-user/benchmark-results/latency-log)" [queueSize=1024];
}
HINT: You can use graphviz or http://viz-js.com to visualize the printed graph.
```

On one of the `Jet-*` nodes you'll see the latency reports:
```text
Latency 54 ms (first seen key: PLAA, count 1,000)
Latency 76 ms (first seen key: FWAA, count 1,000)
Latency 63 ms (first seen key: RMAA, count 999)
...
```

On one of the `Jet-*` nodes you'll see the benchmark progress messages:
```text
warming up -- 520,000
warming up -- 518,000
warming up -- 516,000
...
480,000
470,000
...
10,000
0
benchmarking is done -- -1,000
benchmarking is done -- -2,000
benchmarking is done -- -3,000
...
```

When you see the "benchmarking is done" message, use Ctrl-C to first
kill the event producer, then the `jet submit` process, and then the
entire Jet cluster.

## Collect the Benchmark Results

The results may be on any Jet node. To find them, do this on all the
`Jet-*` instances:

```bash
$ ls -l benchmark-results/*
benchmark-results/latency-log:
total 0

benchmark-results/latency-profile:
total 4
-rw-rw-r-- 1 ec2-user ec2-user 4048 Oct  5 13:41 2
```

In the example above, the instance has recorded the latency profile (the
file name is just `"2"`), while the latency log is on another instance.
To help keeping track of all the benchmark results, we created the
directories `lat-profile` and `lat-log` and the following script after
each run:

```bash
#!/bin/bash
if [ $1 == '' ]; then
	echo missing param
	exit
fi
cd
if [ -f lat-profile/profile-$1.txt -o -f lat-log/log-$1.txt ]; then
	echo $1 already exists
	exit
fi
cd benchmark-results
mv latency-profile/? ~/lat-profile/profile-$1.txt
mv latency-log/? ~/lat-log/log-$1.txt
```

It expects a parameter, a unique string identifying the benchmark, for
example `001`.

To visualize a latency profile (it is in HdrHistogram's format), you can
go to [this
page](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html).

To visualize the raw latency log, you can use `gnuplot` :

```bash
$ cd /path/to/hazelcast-jet/benchmark-results/latency-log
$ gnuplot -e "set datafile separator ','; plot '0'; pause -1"
```

## Clean up Kafka

Since the benchmark produces a lot of data to Kafka, which holds on to
it for a long time, it is a good idea to clean up Kafka storage after
each run. Do this on all three `Kafka-*` instances:

```sh
<Type Ctrl-C>
...
[2020-10-05 13:56:28,939] INFO [KafkaServer id=1001] shut down completed (kafka.server.KafkaServer)
```

Then, on `Kafka-1`, shut down ZooKeeper:

```sh
$ fg
<Type Ctrl-C>
```

Then, on all three `Kafka-*` instances, delete all the data:

```sh
$ rm -r ~/big/*
```

The Kafka topic metadata is stored in ZooKeeper so you don't have to
re-create it.

This benchmark was used to create results described in [this blog post](https://jet-start.sh/blog/2021/03/17/billion-events-per-second).

To achieve same results you need to create 45 instances of ```c5.4xlarge``` on AWS, and run the
```Q05HotItems``` benchmark with settings:

```properties
events-per-second=1_000_000_000
num-distinct-keys=10_000
window-size-millis=10_000
sliding-step-millis=20
processing-guarantee=none
snapshot-interval-millis=1_000
warmup-seconds=150
measurement-seconds=600
latency-reporting-threshold-millis=10
output-path=benchmark-results
```

And ```jvm.options``` in HZ cluster:
```
-Xms24g
-Xmx24g
-XX:+AlwaysPreTouch
-XX:MaxMetaspaceSize=256m
-XX:MaxGCPauseMillis=50
```
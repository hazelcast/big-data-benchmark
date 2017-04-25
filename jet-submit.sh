#!/bin/sh

JAR_FILE=$1
shift
java -cp jet-submit-0.1-SNAPSHOT.jar:$JAR_FILE com.hazelcast.jet.JobSubmitter $JAR_FILE $@

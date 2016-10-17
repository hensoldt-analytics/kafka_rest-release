#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
KAFKA_REST_HOME=$bin/..

NOW=`date "+%Y%m%d%H%M%S"`
GC_OPTS="-XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -Xloggc:$bin/../logs/gc.log.$NOW"
JAVA_OPTS="-Xms1024m -Xmx1024m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=3 -XX:+UseCompressedOops $GC_OPTS"

if [ -d "/var/run/kafka-rest-api" ]; then
    PIDFILE="/var/run/kafka-rest-api/kafka-rest-api.pid"
else
    PIDFILE="$bin/../kafka-rest-api.pid"
fi

# create logs dir if it doesn't exist
if [ ! -d $bin/../logs ]; then
    mkdir -p $bin/../logs
fi

# if this is a developer then use the main jar in the build directory
if [ -d $bin/../target ]; then
    MAIN_JAR_PATH="$bin/../target/rest-api-*.jar"
    if [ "$DAEMON_DETACHED" = "" ]; then
        DAEMON_DETACHED=true
    fi
else
    MAIN_JAR_PATH="$bin/../rest-api-*.jar"
    if [ "$DAEMON_DETACHED" = "" ]; then
        DAEMON_DETACHED=true
    fi
fi

# add main jar
for lib in `ls $MAIN_JAR_PATH`; do
    CLASSPATH=${CLASSPATH}:$lib
done

# add dependency libs
if [  -d $bin/../lib ]; then
 for lib in `ls $bin/../lib/*.jar`; do
    CLASSPATH=${CLASSPATH}:$lib
 done
fi


if [ "$DAEMON_DETACHED" = false ]; then
    java $JAVA_OPTS -cp $CLASSPATH "org.apache.kafka.rest.KafkaRestApplication" "server" $KAFKA_REST_HOME/conf/kafka-rest.yml
    RETVAL=$?
else
    nohup java $JAVA_OPTS -cp $CLASSPATH "org.apache.kafka.rest.KafkaRestApplication" "server" $KAFKA_REST_HOME/conf/kafka-rest.yml > $bin/../logs/kafkarestapi.out 2>&1 < /dev/null &
    PID=$!
    RETVAL=$?
    echo $PID > $PIDFILE
    echo "Started Rest Proxy!"
fi

exit $RETVAL

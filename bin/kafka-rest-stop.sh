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

if [ -d "/var/run/kafka-rest-api" ]; then
    PID_FILE="/var/run/kafka-rest-api/kafka-rest-api.pid"
else
    PID_FILE="$bin/../kafka-rest-api.pid"
fi

if [ ! -f $PID_FILE ]; then
   PID=0
   return 1
fi
PID="$(<$PID_FILE)"
echo "Stopping Kafka Rest Proxy!"
kill $PID 

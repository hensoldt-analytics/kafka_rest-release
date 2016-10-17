/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.rest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.util.Properties;

public class KafkaRestConfig extends Configuration {

    public final static String ID = "id";
    public final static String CONSUMER_POLL_TIMEOUT = "simpleconsumer.poll.timeout";
    public final static String CONSUMER_POOL_MAX_SIZE = "simpleconsumer.pool.max.size";
    public final static String CONSUMER_POOL_INSTANCE_TIMEOUT = "simpleconsumer.pool.instance.timeout";


    @NotEmpty
    private Properties producer = new Properties();

    @NotEmpty
    private Properties consumer = new Properties();

    @NotEmpty
    private Properties restconfig = new Properties();

    @NotEmpty
    private String zookeeperHost;
    
    public Properties getProducer() { return producer; }

    public Properties getConsumer() { return consumer; }

    public Properties getRestconfig() { return restconfig; }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public int getInt(String key) {
        String intString = restconfig.getProperty(key);
        return new Integer(intString.trim()).intValue();
    }

    public int getInt(String key, String defaultValue) {
        String intString = restconfig.getProperty(key, defaultValue);
        return new Integer(intString.trim()).intValue();
    }

    public int getLong(String key) {
        String longString = restconfig.getProperty(key);
        return new Long(longString.trim()).intValue();
    }
    public int getLong(String key, String defaultValue) {
        String longString = restconfig.getProperty(key, defaultValue);
        return new Long(longString.trim()).intValue();
    }
}

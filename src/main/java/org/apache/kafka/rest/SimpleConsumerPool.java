/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.rest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumerPool {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumerPool.class);

    private AtomicInteger CLIENT_ID_COUNTER = new AtomicInteger(0);

    // maxPoolSize = 0 means unlimited
    private final int maxPoolSize;
    // poolInstanceAvailabilityTimeoutMs = 0 means there is no timeout
    private final int poolInstanceAvailabilityTimeoutMs;
    private KafkaRestConfig restConfig;

    private final Map<String, SimpleConsumer> simpleConsumers;
    private final Queue<String> availableConsumers;

    public SimpleConsumerPool(KafkaRestConfig restConfig) {
        this.restConfig = restConfig;
        this.maxPoolSize = restConfig.getInt(KafkaRestConfig.CONSUMER_POOL_MAX_SIZE);
        this.poolInstanceAvailabilityTimeoutMs = restConfig.getInt(KafkaRestConfig.CONSUMER_POOL_INSTANCE_TIMEOUT);;
        simpleConsumers = new HashMap<String, SimpleConsumer>();
        availableConsumers = new LinkedList<String>();
    }

    synchronized public SimpleConsumer get() {

        final long expiration = System.currentTimeMillis() + poolInstanceAvailabilityTimeoutMs;

        while (true) {
            // If there is a SimpleConsumer available
            if (availableConsumers.size() > 0) {
                final String consumerId = availableConsumers.remove();
                return simpleConsumers.get(consumerId);
            }

            // If not consumer is available, but we can instantiate a new one
            if (simpleConsumers.size() < maxPoolSize || maxPoolSize == 0) {
                SimpleConsumer consumer = createConsumer();
                simpleConsumers.put(consumer.clientId(), consumer);
                return consumer;
            }

            // If no consumer is available and we reached the limit
            try {
                // The behavior of wait when poolInstanceAvailabilityTimeoutMs=0 is consistent as it won't timeout
                wait(poolInstanceAvailabilityTimeoutMs);
            } catch (InterruptedException e) {
                log.warn("A thread requesting a SimpleConsumer has been interrupted while waiting", e);
            }

            // In some cases ("spurious wakeup", see wait() doc), the thread will resume before the timeout
            // We have to guard against that and throw only if the timeout has expired for real
            if (System.currentTimeMillis() > expiration && poolInstanceAvailabilityTimeoutMs != 0) {
                throw new TimeoutException("");
            }
        }

    }

    synchronized public void release(SimpleConsumer simpleConsumer) {
        log.debug("Releasing into the pool SimpleConsumer with id " + simpleConsumer.clientId());
        availableConsumers.add(simpleConsumer.clientId());
        notify();
    }

    public void shutdown() {
        for (SimpleConsumer simpleConsumer : simpleConsumers.values()) {
            simpleConsumer.consumer().close();
        }
    }

    private SimpleConsumer createConsumer() {
        String clientId = getClientId();
        log.debug("Creating Simple Consumer with ID:" + clientId);
        Properties consumerProps = restConfig.getConsumer();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, clientId);

        return new SimpleConsumer(new KafkaConsumer<byte[], byte[]>(consumerProps), clientId);
    }

    private String getClientId() {
        StringBuilder id = new StringBuilder();
        id.append("ManualAssignConsumer-");
        String serverId = restConfig.getRestconfig().getProperty(KafkaRestConfig.ID);
        if (!serverId.isEmpty()) {
            id.append(serverId);
            id.append("-");
        }
        id.append(Integer.toString(CLIENT_ID_COUNTER.incrementAndGet()));
        return id.toString();
    }
}

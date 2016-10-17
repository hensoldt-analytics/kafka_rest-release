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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.rest.entities.BinaryConsumerRecord;
import org.apache.kafka.rest.entities.ConsumerBaseRecord;
import org.apache.kafka.rest.entities.EmbeddedDataFormat;
import org.apache.kafka.rest.entities.EntityUtils;
import org.apache.kafka.rest.entities.JsonConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.AsyncResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SimpleConsumerManager {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumerManager.class);

    private KafkaRestConfig restConfig;
    private SimpleConsumerPool consumerPool;

    public SimpleConsumerManager(KafkaRestConfig restConfig) {
        this.restConfig = restConfig;
        this.consumerPool = new SimpleConsumerPool(restConfig);
    }

    public void consume(AsyncResponse asyncResponse, String topic, int partition, long offset,
                        int count, EmbeddedDataFormat embeddedDataFormat) {

        List<ConsumerBaseRecord> records = new ArrayList<>();
        SimpleConsumer simpleConsumer = null;

        try {
            long pollTimeout = restConfig.getLong(KafkaRestConfig.CONSUMER_POLL_TIMEOUT);
            simpleConsumer = consumerPool.get();
            KafkaConsumer<byte[], byte[]> consumer = simpleConsumer.consumer();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seek(topicPartition, offset);

            while (count > 0) {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(pollTimeout);
                List<ConsumerRecord<byte[], byte[]>> partitionRecords = consumerRecords.records(topicPartition);

                if (partitionRecords.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                    records.add(createConsumerRecord(record, embeddedDataFormat));
                    count--;

                    if (count == 0) {
                        break;
                    }
                }
            }

        }
        //Catch Exceptions
        finally {
            if (simpleConsumer != null) {
                try {
                    consumerPool.release(simpleConsumer);
                } catch (Exception e) {
                    log.error("Unable to release ManualConsumer into the pool", e);
                }
            }

        }

        asyncResponse.resume(records);
    }

    private ConsumerBaseRecord createConsumerRecord(final ConsumerRecord<byte[], byte[]> record,
                                                    final EmbeddedDataFormat embeddedDataFormat) {
        switch (embeddedDataFormat) {
            case BINARY:
                return new BinaryConsumerRecord(record.key(), record.value(), record.partition(), record.offset());

            case JSON:
                return new JsonConsumerRecord(EntityUtils.jsonDeSerialize(record.key()), EntityUtils.jsonDeSerialize(record.value()),
                        record.partition(), record.offset());

            default:
                throw new IllegalArgumentException("Invalid embedded format ");
        }
    }

    public void close() {
        consumerPool.shutdown();
    }
}

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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.rest.entities.BinaryProduceRecord;
import org.apache.kafka.rest.entities.EntityUtils;
import org.apache.kafka.rest.entities.JsonProduceRecord;
import org.apache.kafka.rest.entities.ProduceRecordResponse;
import org.apache.kafka.rest.entities.ProduceResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerManager {

    private KafkaRestConfig restConfig;
    private KafkaProducer producer;

    public ProducerManager(KafkaRestConfig restConfig) {
        this.restConfig = restConfig;
        Properties producerProps = restConfig.getProducer();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer(producerProps);
    }

    public ProduceResponse produceBinary(String topic, Integer partition, BinaryProduceRecord[] records) {
        ProduceResponse produceResponse = new ProduceResponse();
        produceResponse.setOffsets(produce(getProducerRecords(topic, partition, records)));
        return produceResponse;
    }

    public ProduceResponse produceJson(String topic, Integer partition, JsonProduceRecord[] records) {
        ProduceResponse produceResponse = new ProduceResponse();
        produceResponse.setOffsets(produce(getProducerRecords(topic, partition, records)));
        return produceResponse;
    }

    private List<ProducerRecord<byte[], byte[]>> getProducerRecords(String topic, Integer partition, BinaryProduceRecord[] records) {
        List<ProducerRecord<byte[], byte[]>> producerRecords = new LinkedList<>();
        for (BinaryProduceRecord record : records) {
            Integer recordPartition = partition;
            if (recordPartition == null) {
                recordPartition = record.partition();
            }
            producerRecords.add(new ProducerRecord<>(topic, recordPartition, record.getKey(), record.getValue()));
        }
        return producerRecords;
    }

    private List<ProducerRecord<byte[], byte[]>> getProducerRecords(String topic, Integer partition, JsonProduceRecord[] records) {
        List<ProducerRecord<byte[], byte[]>> producerRecords = new LinkedList<>();
        for (JsonProduceRecord record : records) {
            byte[] keyBytes = (record.getKey() != null) ? EntityUtils.jsonSerialize(record.getKey()) : null;
            byte[] valueBytes = (record.getValue() != null) ? EntityUtils.jsonSerialize(record.getValue()) : null;

            Integer recordPartition = partition;
            if (recordPartition == null) {
                recordPartition = record.partition();
            }
            producerRecords.add(new ProducerRecord<>(topic, recordPartition, keyBytes, valueBytes));
        }
        return producerRecords;
    }

    public List<ProduceRecordResponse> produce(List<ProducerRecord<byte[], byte[]>> producerRecords) {
        List<Future<RecordMetadata>> futureList = new LinkedList<>();
        for (ProducerRecord<byte[], byte[]> record : producerRecords) {
            futureList.add(producer.send(record));
        }

        List<ProduceRecordResponse> responses = new LinkedList<>();
        for (Future<RecordMetadata> future : futureList) {
            RecordMetadata recordMetadata = null;

            try {
                recordMetadata = future.get();
                responses.add(new ProduceRecordResponse(recordMetadata.partition(), recordMetadata.offset(), null, null));
            } catch (Exception e) {
                Integer errorCode = 0; //TODO
                String errorMessage = e.getMessage();
                responses.add(new ProduceRecordResponse(null, null, errorCode, errorMessage));
            }
        }

        return responses;
    }

    public void close() {
        producer.close();
    }

}

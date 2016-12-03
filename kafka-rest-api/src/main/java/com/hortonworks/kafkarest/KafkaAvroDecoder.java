/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.kafkarest;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class KafkaAvroDecoder extends AvroSnapshotDeserializer implements Decoder<Object> {

  public KafkaAvroDecoder(VerifiableProperties verifiableProperties) {
    Map<String, Object> config = new HashMap<>();
    for (Map.Entry<Object, Object> entry : verifiableProperties.props().entrySet()) {
      config.put(entry.getKey().toString(), entry.getValue());
    }

    init(config);
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    return deserialize(new ByteArrayInputStream(bytes), null, null);
  }
}

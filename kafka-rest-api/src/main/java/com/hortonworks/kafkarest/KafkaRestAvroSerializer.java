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

import org.apache.avro.Schema;
import org.apache.registries.schemaregistry.SchemaIdVersion;
import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.SchemaMetadataInfo;
import org.apache.registries.schemaregistry.SchemaVersion;
import org.apache.registries.schemaregistry.SchemaVersionInfo;
import org.apache.registries.schemaregistry.SchemaVersionKey;
import org.apache.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.registries.schemaregistry.errors.IncompatibleSchemaException;
import org.apache.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;

import java.util.Map;

/**
 *
 */
public class KafkaRestAvroSerializer extends KafkaAvroSerializer {

  private SchemaRegistryClient schemaRegistryClient;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    schemaRegistryClient = new SchemaRegistryClient(configs);
  }

  @Override
  protected SchemaMetadata getSchemaKey(String topic, boolean isKey) {
    String name = topic + (isKey ? "-key" : "-value");
    return new SchemaMetadata.Builder(name).type("avro").schemaGroup("kafka").build();
  }

  public Schema getByID(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
    SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(schemaIdVersion.getSchemaMetadataId());
    SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaMetadataInfo.getSchemaMetadata().getName(), schemaIdVersion.getVersion()));
    String schemaText = schemaVersionInfo.getSchemaText();
    return new Schema.Parser().parse(schemaText);
  }

  public SchemaIdVersion register(String schemaName, Schema schema) throws SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException {
    SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(schemaName)
                                      .type("avro")
                                      .description("SchemaMetadata is registered by KafkaRestAvroSerializer")
                                      .build();
    SchemaVersion schemaVersion = new SchemaVersion(schema.toString(), "Schemaversion added KafkaRestAvroSerializer");
    return schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaVersion);
  }
}

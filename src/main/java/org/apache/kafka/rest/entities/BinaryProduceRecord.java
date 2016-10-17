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

package org.apache.kafka.rest.entities;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import java.io.IOException;
import java.util.Arrays;

public class BinaryProduceRecord {

    @JsonProperty
    protected byte[] key;

    @JsonProperty
    protected byte[] value;

    @JsonCreator
    public BinaryProduceRecord(@JsonProperty("key") String key,
                               @JsonProperty("value") String value) throws IOException {
        try {
            this.key = (key != null) ? EntityUtils.parseBase64Binary(key) : null;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Record key contains invalid base64 encoding");
        }
        try {
            this.value = (value != null) ? EntityUtils.parseBase64Binary(value) : null;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Record value contains invalid base64 encoding");
        }
    }

    public BinaryProduceRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public BinaryProduceRecord(byte[] value) {
        this(null, value);
    }

    @JsonIgnore
    public byte[] getKey() {
        return key;
    }

    @JsonIgnore
    public byte[] getValue() {
        return value;
    }

    @JsonProperty("key")
    public String getJsonKey() {
        return (key == null ? null : EntityUtils.encodeBase64Binary(key));
    }

    @JsonProperty("value")
    public String getJsonValue() {
        return (value == null ? null : EntityUtils.encodeBase64Binary(value));
    }

    public Integer partition() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinaryProduceRecord that = (BinaryProduceRecord) o;

        if (!Arrays.equals(key, that.key)) return false;
        return Arrays.equals(value, that.value);

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
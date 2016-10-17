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

public class JsonTopicProduceRecord extends JsonProduceRecord {

    @Min(0)
    @JsonProperty
    private Integer partition;

    @JsonCreator
    public JsonTopicProduceRecord(@JsonProperty("key") Object key,
                                  @JsonProperty("value") Object value,
                                  @JsonProperty("partition") Integer partition) throws IOException {
        super(key, value);
        this.partition = partition;
    }

    @Override
    @JsonProperty("partition")
    public Integer partition() {
        return partition;
    }

    @JsonProperty
    public Integer getPartition() {
        return partition;
    }

    @JsonProperty
    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        JsonTopicProduceRecord that = (JsonTopicProduceRecord) o;

        return partition != null ? partition.equals(that.partition) : that.partition == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        return result;
    }
}
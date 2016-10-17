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

package org.apache.kafka.rest.resources;

import com.codahale.metrics.annotation.Timed;
import org.apache.kafka.rest.SimpleConsumerManager;
import org.apache.kafka.rest.ProducerManager;
import org.apache.kafka.rest.Versions;
import org.apache.kafka.rest.entities.BinaryProduceRecord;
import org.apache.kafka.rest.entities.EmbeddedDataFormat;
import org.apache.kafka.rest.entities.JsonProduceRecord;
import org.apache.kafka.rest.entities.ProduceResponse;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V1_JSON, Versions.JSON})
@Consumes({Versions.KAFKA_V1_JSON, Versions.JSON})
public class PartitionsResource {

    private static final Logger log = LoggerFactory.getLogger(PartitionsResource.class);

    private ProducerManager producerManager;
    private SimpleConsumerManager simpleConsumerManager;

    public PartitionsResource(ProducerManager producerManager, SimpleConsumerManager simpleConsumerManager) {
        this.producerManager = producerManager;
        this.simpleConsumerManager = simpleConsumerManager;
    }

    @POST
    @Timed
    @Consumes({Versions.KAFKA_V1_JSON_BINARY})
    @Path("/{partition}")
    @ManagedAsync
    public void produceBinary(final @Suspended AsyncResponse asyncResponse,
                              @PathParam("topic") String topic,
                              @PathParam("partition") int partition,
                              @Valid BinaryProduceRecord[] records) {
        log.debug("Binary produce request:  topic : {} partition : {} records size: {}", topic, records.length);
        ProduceResponse response = producerManager.produceBinary(topic, partition, records);
        asyncResponse.resume(response);
    }

    @POST
    @Timed
    @Consumes({Versions.KAFKA_V1_JSON_JSON})
    @Path("/{partition}")
    @ManagedAsync
    public void produceJson(final @Suspended AsyncResponse asyncResponse,
                            @PathParam("topic") String topic,
                            @PathParam("partition") int partition,
                            @Valid JsonProduceRecord[] records) {
        log.debug("Json produce request:  topic : {} partition : {} records size: {}", topic, records.length);
        ProduceResponse response =  producerManager.produceJson(topic, partition, records);
        asyncResponse.resume(response);
    }

    @GET
    @Timed
    @Path("/{partition}")
    @Produces({Versions.KAFKA_V1_JSON_BINARY})
    @ManagedAsync
    public void consumeBinary(final @Suspended AsyncResponse asyncResponse,
                              final @PathParam("topic") String topic,
                              final @PathParam("partition") int partition,
                              final @QueryParam("offset") long offset,
                              final @QueryParam("count") @DefaultValue("1") int count) {
        log.debug("Binary consume request: topic : {} partition : {}", topic, partition);
        simpleConsumerManager.consume(asyncResponse, topic, partition, offset, count, EmbeddedDataFormat.BINARY);
    }

    @GET
    @Timed
    @Path("/{partition}")
    @Produces({Versions.KAFKA_V1_JSON_JSON})
    @ManagedAsync
    public void consumeJson(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("topic") String topic,
                            final @PathParam("partition") int partition,
                            final @QueryParam("offset") long offset,
                            final @QueryParam("count") @DefaultValue("1") int count) {
        log.debug("Json consume request: topic : {} partition : {}", topic, partition);
        simpleConsumerManager.consume(asyncResponse, topic, partition, offset, count, EmbeddedDataFormat.JSON);
    }
}

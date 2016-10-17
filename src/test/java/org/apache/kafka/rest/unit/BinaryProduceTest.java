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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.rest.unit;

import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.kafka.rest.ProducerManager;
import org.apache.kafka.rest.Versions;
import org.apache.kafka.rest.entities.BinaryProduceRecord;
import org.apache.kafka.rest.entities.ProduceRecordResponse;
import org.apache.kafka.rest.entities.ProduceResponse;
import org.apache.kafka.rest.resources.TopicsResource;
import org.easymock.EasyMock;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class BinaryProduceTest {

    private static final ProducerManager producerManager = EasyMock.createMock(ProducerManager.class);

    List<ProduceRecordResponse> offsetResults = Arrays.asList(
            new ProduceRecordResponse(0, 0L, null, null),
            new ProduceRecordResponse(0, 1L, null, null)
    );

    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new TopicsResource(producerManager))
            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
            .build();

    @Before
    public void setup() {
        EasyMock.reset(producerManager);
    }

    @Test
    public void testBinaryProduce() {
        List<BinaryProduceRecord> produceRecordsOnlyValues = Arrays.asList(
                new BinaryProduceRecord("value".getBytes()),
                new BinaryProduceRecord("value2".getBytes())
        );

        ProduceResponse mockResponse = new ProduceResponse();
        mockResponse.setOffsets(offsetResults);

        EasyMock.expect(producerManager.produceBinary(EasyMock.eq("topic"),
                EasyMock.<Integer>anyObject(),
                (BinaryProduceRecord[]) EasyMock.anyObject())).
                andReturn(mockResponse);
        EasyMock.replay(producerManager);

        Response response = resources.getJerseyTest().target("/topics/topic").request()
                .post(Entity.entity(produceRecordsOnlyValues.toArray(), Versions.KAFKA_V1_JSON_BINARY), Response.class);

        assertOKResponse(response, Versions.KAFKA_V1_JSON);
        ProduceResponse produceResponse = response.readEntity(ProduceResponse.class);
        assertEquals(offsetResults, produceResponse.getOffsets());
        EasyMock.verify(producerManager);
        EasyMock.reset(producerManager);
    }

    public static void assertOKResponse(Response rawResponse, String mediatype) {
        assertEquals(Response.Status.OK.getStatusCode(), rawResponse.getStatus());
        assertEquals(mediatype, rawResponse.getMediaType().toString());
    }
}
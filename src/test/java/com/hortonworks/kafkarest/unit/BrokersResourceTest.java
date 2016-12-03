/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.kafkarest.unit;

import com.hortonworks.kafkarest.MetadataObserver;
import com.hortonworks.kafkarest.entities.BrokerList;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import com.hortonworks.kafkarest.Context;
import com.hortonworks.kafkarest.KafkaRestApplication;
import com.hortonworks.kafkarest.KafkaRestConfig;
import com.hortonworks.kafkarest.ProducerPool;
import com.hortonworks.kafkarest.TestUtils;
import com.hortonworks.kafkarest.resources.BrokersResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;

import static com.hortonworks.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class BrokersResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private MetadataObserver mdObserver;
  private ProducerPool producerPool;
  private Context ctx;

  public BrokersResourceTest() throws RestConfigException {
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new Context(config, mdObserver, producerPool, null, null);
    addResource(new BrokersResource(ctx));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(mdObserver, producerPool);
  }

  @Test
  public void testList() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      final List<Integer> brokerIds = Arrays.asList(1, 2, 3);
      EasyMock.expect(mdObserver.getBrokerIds()).andReturn(brokerIds);
      EasyMock.replay(mdObserver);

      Response response = request("/brokers", mediatype.header).get();
      assertOKResponse(response, mediatype.expected);
      final BrokerList returnedBrokerIds = response.readEntity(new GenericType<BrokerList>() {
      });
      assertEquals(brokerIds, returnedBrokerIds.getBrokers());
      EasyMock.verify(mdObserver);
      EasyMock.reset(mdObserver, producerPool);
    }
  }
}

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
import org.apache.kafka.rest.ClusterManager;
import org.apache.kafka.rest.entities.BrokerList;
import org.apache.kafka.rest.resources.ClusterResource;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class ClusterResourceTest {

    private static final ClusterManager clusterManager = EasyMock.createMock(ClusterManager.class);

    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new ClusterResource(clusterManager))
            .build();

    @Before
    public void setup() {
        EasyMock.reset(clusterManager);
    }

    @Test
    public void testGetBrokerList() {
        final List<Integer> brokerIds = Arrays.asList(1, 2, 3);
        EasyMock.expect(clusterManager.getBrokerIds()).andReturn(brokerIds);
        EasyMock.replay(clusterManager);

        final BrokerList returnedBrokerIds = resources.client().target("/cluster/brokers").request().get(BrokerList.class);
        assertEquals(brokerIds, returnedBrokerIds.getBrokers());
    }
}

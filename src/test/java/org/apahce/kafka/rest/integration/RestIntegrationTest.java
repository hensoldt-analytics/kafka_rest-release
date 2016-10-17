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

package org.apahce.kafka.rest.integration;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.kafka.rest.KafkaRestApplication;
import org.apache.kafka.rest.KafkaRestConfig;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class RestIntegrationTest {

    @ClassRule
    public static final DropwizardAppRule<KafkaRestConfig> RULE = new DropwizardAppRule<>(KafkaRestApplication.class, ResourceHelpers.resourceFilePath("kafka-rest.yml"));

    @Test
    public void testTopicResource() throws Exception {
       //Add some tests
    }
}

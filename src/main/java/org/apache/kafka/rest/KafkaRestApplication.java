/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.rest;

import com.codahale.metrics.health.HealthCheck;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.kafka.rest.resources.ClusterResource;
import org.apache.kafka.rest.resources.PartitionsResource;
import org.apache.kafka.rest.resources.TopicsResource;
import org.glassfish.jersey.filter.LoggingFilter;
import java.util.logging.Logger;

public class KafkaRestApplication extends Application<KafkaRestConfig> implements Managed {

    private ProducerManager producerManager;
    private SimpleConsumerManager simpleConsumerManager;
    private ClusterManager clusterManager;

    @Override
    public String getName() { return "kafka-http"; }

    @Override
    public void initialize(Bootstrap<KafkaRestConfig> bootstrap) {}

    @Override
    public void run(KafkaRestConfig configuration, Environment environment) {

        producerManager = new ProducerManager(configuration);
        clusterManager = new ClusterManager(configuration);
        simpleConsumerManager = new SimpleConsumerManager(configuration);
        environment.jersey().register(new ClusterResource(clusterManager));
        environment.jersey().register(new TopicsResource(producerManager));
        environment.jersey().register(new PartitionsResource(producerManager, simpleConsumerManager));
        environment.healthChecks().register("empty", new EmptyHealthCheck());
        environment.jersey().register(new LoggingFilter(
                Logger.getLogger(LoggingFilter.class.getName()),
                true)
        );

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run()
            {
                producerManager.close();
                simpleConsumerManager.close();
                clusterManager.close();
            }
        });

    }

    @Override
    public void start() throws Exception {}

    @Override
    public void stop() throws Exception {}

    private static class EmptyHealthCheck extends HealthCheck {
        @Override
        protected Result check() throws Exception {
            return Result.healthy();
        }
    }

    public static void main(String[] args) throws Exception {
        new KafkaRestApplication().run(args);
    }
}

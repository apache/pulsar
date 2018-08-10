/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.tests.integration.suites;

import java.util.Map;
import org.apache.pulsar.tests.integration.containers.CassandraContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec.PulsarClusterSpecBuilder;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testng.ITest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.collections.Maps;

public class PulsarTestSuite extends PulsarClusterTestBase implements ITest {

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    @Override
    protected PulsarClusterSpecBuilder beforeSetupCluster(String clusterName, PulsarClusterSpecBuilder specBuilder) {
        PulsarClusterSpecBuilder builder = super.beforeSetupCluster(clusterName, specBuilder);

        // start functions

        // register external services
        Map<String, GenericContainer<?>> externalServices = Maps.newHashMap();
        final String kafkaServiceName = "kafka";
        externalServices.put(
            kafkaServiceName,
            new KafkaContainer()
                .withEmbeddedZookeeper()
                .withNetworkAliases(kafkaServiceName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                    .withName(kafkaServiceName)
                    .withHostName(clusterName + "-" + kafkaServiceName)));
        final String cassandraServiceName = "cassandra";
        externalServices.put(
            cassandraServiceName,
            new CassandraContainer(clusterName));
        builder = builder.externalServices(externalServices);

        return builder;
    }

    @Override
    public String getTestName() {
        return "pulsar-test-suite";
    }
}

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
package org.apache.pulsar.tests.integration.io.sources.debezium;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.integration.containers.DebeziumOracleDbContainer;
import org.apache.pulsar.tests.integration.io.PulsarIOTestBase;
import org.testcontainers.shaded.com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PulsarDebeziumOracleSourceTest extends PulsarIOTestBase {

    protected final AtomicInteger testId = new AtomicInteger(0);

    @Test(groups = "source", timeOut = 1800000)
    public void testDebeziumOracleDbSource() throws Exception{
        testDebeziumOracleDbConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    private void testDebeziumOracleDbConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name-" + testId.getAndIncrement();
        final String consumeTopicName = "debezium/oracle/XE.INV.CUSTOMERS";
        final String sourceName = "test-source-debezium-oracle-" + functionRuntimeType + "-" + randomName(8);

        // This is the event count to be created by prepareSource.
        final int numMessages = 1;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        initNamespace(admin);

        admin.topics().createNonPartitionedTopic(consumeTopicName);
        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        DebeziumOracleDbSourceTester sourceTester = new DebeziumOracleDbSourceTester(pulsarCluster);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium oracle server
        DebeziumOracleDbContainer debeziumOracleDbContainer = new DebeziumOracleDbContainer(pulsarCluster.getClusterName());
        sourceTester.setServiceContainer(debeziumOracleDbContainer);

        PulsarIODebeziumSourceRunner runner = new PulsarIODebeziumSourceRunner(pulsarCluster, functionRuntimeType.toString(),
                converterClassName, tenant, namespace, sourceName, outputTopicName, numMessages, jsonWithEnvelope,
                consumeTopicName, client);

        runner.testSource(sourceTester);
    }

    protected void initNamespace(PulsarAdmin admin) {
        log.info("[initNamespace] start.");
        try {
            admin.tenants().createTenant("debezium", new TenantInfoImpl(Sets.newHashSet(),
                    Sets.newHashSet(pulsarCluster.getClusterName())));
            String [] namespaces = {
                "debezium/oracle"
            };
            Policies policies = new Policies();
            policies.retention_policies = new RetentionPolicies(-1, 50);
            for (String ns: namespaces) {
                admin.namespaces().createNamespace(ns, policies);
            }
        } catch (Exception e) {
            log.info("[initNamespace] msg: {}", e.getMessage());
        }
        log.info("[initNamespace] finish.");
    }
}

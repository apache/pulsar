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

import com.google.common.base.Preconditions;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.integration.io.sources.PulsarIOSourceRunner;
import org.apache.pulsar.tests.integration.io.sources.SourceTester;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.containers.GenericContainer;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;

@Slf4j
public class PulsarIODebeziumSourceRunner extends PulsarIOSourceRunner {

    private String converterClassName;
    private String tenant;
    private String namespace;
    private String sourceName;
    private String outputTopicName;
    private String consumeTopicName;
    private int numMessages;
    private boolean jsonWithEnvelope;
    private PulsarClient client;
    
    public PulsarIODebeziumSourceRunner(PulsarCluster cluster, String functionRuntimeType, String converterClassName,
            String tenant, String ns, String sourceName, String outputTopic, int numMessages, boolean jsonWithEnvelope,
            String consumeTopicName, PulsarClient client) {
        super(cluster, functionRuntimeType);
        this.converterClassName = converterClassName;
        this.tenant = tenant;
        this.namespace = ns;
        this.sourceName = sourceName;
        this.outputTopicName = outputTopic;
        this.numMessages = numMessages;
        this.jsonWithEnvelope = jsonWithEnvelope;
        this.consumeTopicName = consumeTopicName;
        this.client = client;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends GenericContainer> void testSource(SourceTester<T> sourceTester)  throws Exception {
           // prepare the testing environment for source
        prepareSource(sourceTester);

        // submit the source connector
        submitSourceConnector(sourceTester, tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(sourceTester, tenant, namespace, sourceName);

        // get source status
        Failsafe.with(statusRetryPolicy).run(() -> getSourceStatus(tenant, namespace, sourceName));

        // wait for source to process messages
        Failsafe.with(statusRetryPolicy).run(() ->
                waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages));

        @Cleanup
        Consumer consumer = client.newConsumer(getSchema(jsonWithEnvelope))
                .topic(consumeTopicName)
                .subscriptionName("debezium-source-tester")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        log.info("[debezium mysql test] create consumer finish. converterName: {}", converterClassName);

        // validate the source result
        sourceTester.validateSourceResult(consumer, sourceTester.getNumEntriesExpectAfterStart(), null, converterClassName);

        final int numEntriesToInsert = sourceTester.getNumEntriesToInsert();
        Preconditions.checkArgument(numEntriesToInsert >= 1);

        for (int i = 1; i <= numEntriesToInsert; i++) {
            // prepare insert event
            sourceTester.prepareInsertEvent();
            log.info("inserted entry {} of {}", i, numEntriesToInsert);
            // validate the source insert event
            sourceTester.validateSourceResult(consumer, 1, SourceTester.INSERT, converterClassName);
        }

        // prepare update event
        sourceTester.prepareUpdateEvent();

        // validate the source update event
        sourceTester.validateSourceResult(consumer, numEntriesToInsert, SourceTester.UPDATE, converterClassName);

        // prepare delete event
        sourceTester.prepareDeleteEvent();

        // validate the source delete event
        sourceTester.validateSourceResult(consumer,  numEntriesToInsert, SourceTester.DELETE, converterClassName);

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }
}

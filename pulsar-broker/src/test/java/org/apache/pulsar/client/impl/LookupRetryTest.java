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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.protocol.Commands.newLookupErrorResponse;
import static org.apache.pulsar.common.protocol.Commands.newPartitionMetadataResponse;

import com.google.common.collect.Sets;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarChannelInitializer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LookupRetryTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(LookupRetryTest.class);
    private static final String subscription = "reader-sub";
    private final AtomicInteger connectionsCreated = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Queue<LookupError>> failureMap = new ConcurrentHashMap<>();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));

        connectionsCreated.set(0);
    }

    @Override
    protected PulsarService newPulsarService(ServiceConfiguration conf) throws Exception {
        return new PulsarService(conf) {
            @Override
            protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
                BrokerService broker = new BrokerService(this, ioEventLoopGroup);
                broker.setPulsarChannelInitializerFactory(
                        (_pulsar, opts) -> {
                            return new PulsarChannelInitializer(_pulsar, opts) {
                                @Override
                                protected ServerCnx newServerCnx(PulsarService pulsar, String listenerName) throws Exception {
                                    connectionsCreated.incrementAndGet();
                                    return new ErrorByTopicServerCnx(pulsar, failureMap);
                                }
                            };
                        });
                return broker;
            }
        };
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    PulsarClient newClient() throws Exception {
        return PulsarClient.builder()
            .serviceUrl(pulsar.getBrokerServiceUrl())
            .connectionTimeout(2, TimeUnit.SECONDS)
            .operationTimeout(1, TimeUnit.SECONDS)
            .lookupTimeout(10, TimeUnit.SECONDS)
            .build();
    }

    @Test
    public void testGetPartitionedMetadataRetries() throws Exception {
        try (PulsarClient client = newClient()) {
            client.getPartitionsForTopic("TIMEOUT:2,OK:10").get();
        }
        try (PulsarClient client = newClient()) {
            client.getPartitionsForTopic("TOO_MANY:2,OK:10").get();
        }
    }

    @Test
    public void testTimeoutRetriesOnPartitionMetadata() throws Exception {
        try (PulsarClient client = newClient();
             Reader<byte[]> reader = client
                .newReader().topic("TIMEOUT:2,OK:3").startMessageId(MessageId.latest)
                .startMessageIdInclusive().readerName(subscription).create()) {
        }
    }

    @Test
    public void testTooManyRetriesOnPartitionMetadata() throws Exception {
        try (PulsarClient client = newClient();
             Reader<byte[]> reader = client
                .newReader().topic("TOO_MANY:2,OK:3").startMessageId(MessageId.latest)
                .startMessageIdInclusive().readerName(subscription).create()) {
        }
    }

    @Test
    public void testTooManyOnLookup() throws Exception {
        try (PulsarClient client = newClient();
             Reader<byte[]> reader = client
                .newReader().topic("OK:1,TOO_MANY:2,OK:3").startMessageId(MessageId.latest)
                .startMessageIdInclusive().readerName(subscription).create()) {
        }
    }

    @Test
    public void testTimeoutOnLookup() throws Exception {
        try (PulsarClient client = newClient();
             Reader<byte[]> reader = client
                .newReader().topic("OK:1,TIMEOUT:2,OK:3").startMessageId(MessageId.latest)
                .startMessageIdInclusive().readerName(subscription).create()) {
        }
    }

    @Test
    public void testManyFailures() throws Exception {
        try (PulsarClient client = newClient();
             Reader<byte[]> reader = client
                .newReader().topic("TOO_MANY:1,TIMEOUT:1,OK:1,TIMEOUT:1,TOO_MANY:1,OK:3")
                .startMessageId(MessageId.latest)
                .startMessageIdInclusive().readerName(subscription).create()) {
        }
    }

    @Test
    public void testProducerTimeoutOnPMR() throws Exception {
        try (PulsarClient client = newClient();
             Producer<byte[]> producer = client.newProducer().topic("TIMEOUT:2,OK:3").create()) {
        }
    }

    @Test
    public void testProducerTooManyOnPMR() throws Exception {
        try (PulsarClient client = newClient();
             Producer<byte[]> producer = client.newProducer().topic("TOO_MANY:2,OK:3").create()) {
        }

    }

    @Test
    public void testProducerTimeoutOnLookup() throws Exception {
        try (PulsarClient client = newClient();
             Producer<byte[]> producer = client.newProducer().topic("OK:1,TIMEOUT:2,OK:3").create()) {
        }
    }

    @Test
    public void testProducerTooManyOnLookup() throws Exception {
        try (PulsarClient client = newClient();
             Producer<byte[]> producer = client.newProducer().topic("OK:1,TOO_MANY:2,OK:3").create()) {
        }
    }

    /**
     * <pre>
     * Verifies: that client-cnx gets closed when server gives TooManyRequestException in certain time frame
     * Client1: which has set MaxNumberOfRejectedRequestPerConnection=1, should fail on TooManyRequests
     * Client2: which has set MaxNumberOfRejectedRequestPerConnection=100, should not fail
     * on TooManyRequests, whether there is 1 or 4 (I don't do more because exponential
     * backoff would make it take a long time.
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testCloseConnectionOnBrokerRejectedRequest() throws Exception {
        String lookupUrl = pulsar.getBrokerServiceUrl();
        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl)
                .maxNumberOfRejectedRequestPerConnection(1).build()) {

            // need 2 TooManyRequests because it takes the count before incrementing
            pulsarClient.newProducer().topic("TOO_MANY:2").create().close();

            Assert.assertEquals(connectionsCreated.get(), 2);
        }

        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl)
             .maxNumberOfRejectedRequestPerConnection(100).build()) {

            pulsarClient.newProducer().topic("TOO_MANY:2").create().close();
            pulsarClient.newProducer().topic("TOO_MANY:4").create().close();
            Assert.assertEquals(connectionsCreated.get(), 3);
        }
    }

    @Test
    public void testCloseConnectionOnBrokerTimeout() throws Exception {
        String lookupUrl = pulsar.getBrokerServiceUrl();
        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl)
                .maxNumberOfRejectedRequestPerConnection(1)
                .connectionTimeout(2, TimeUnit.SECONDS)
                .operationTimeout(1, TimeUnit.SECONDS)
                .lookupTimeout(10, TimeUnit.SECONDS)
                .build()) {

            // need 2 Timeouts because it takes the count before incrementing
            pulsarClient.newProducer().topic("TIMEOUT:2").create().close();

            Assert.assertEquals(connectionsCreated.get(), 2);
        }

        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl)
                .maxNumberOfRejectedRequestPerConnection(100)
                .maxNumberOfRejectedRequestPerConnection(1)
                .connectionTimeout(2, TimeUnit.SECONDS)
                .operationTimeout(1, TimeUnit.SECONDS)
                .lookupTimeout(10, TimeUnit.SECONDS)
                .build()) {

            pulsarClient.newProducer().topic("TIMEOUT:2").create().close();
            pulsarClient.newProducer().topic("TIMEOUT:2").create().close();
            Assert.assertEquals(connectionsCreated.get(), 3);
        }
    }

    enum LookupError {
        UNKNOWN,
        TOO_MANY,
        TIMEOUT,
        OK,
    }

    private static class ErrorByTopicServerCnx extends ServerCnx {
        private final ConcurrentHashMap<String, Queue<LookupError>> failureMap;

        ErrorByTopicServerCnx(PulsarService pulsar, ConcurrentHashMap<String, Queue<LookupError>> failureMap) {
            super(pulsar);
            this.failureMap = failureMap;
        }

        private Queue<LookupError> errorList(String topicName) {
            return failureMap.compute(
                    topicName,
                    (k, v) -> {
                        if (v == null) {
                            v = new ArrayBlockingQueue<LookupError>(100);
                            for (String e : k.split(",")) {
                                String[] parts = e.split(":");
                                LookupError error = Enum.valueOf(LookupError.class, parts[0]);
                                for (int i = 0; i < Integer.parseInt(parts[1]); i++) {
                                    v.add(error);
                                }
                            }
                        }
                        return v;
                    });
        }

        @Override
        protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
            TopicName t = TopicName.get(partitionMetadata.getTopic());
            LookupError error = errorList(t.getLocalName()).poll();
            if (error == LookupError.TOO_MANY) {
                final long requestId = partitionMetadata.getRequestId();
                ctx.writeAndFlush(newPartitionMetadataResponse(ServerError.TooManyRequests, "too many", requestId));
            } else if (error == LookupError.TIMEOUT) {
                // do nothing
            } else if (error == null || error == LookupError.OK) {
                super.handlePartitionMetadataRequest(partitionMetadata);
            }
        }

        @Override
        protected void handleLookup(CommandLookupTopic lookup) {
            TopicName t = TopicName.get(lookup.getTopic());
            LookupError error = errorList(t.getLocalName()).poll();
            if (error == LookupError.TOO_MANY) {
                final long requestId = lookup.getRequestId();
                ctx.writeAndFlush(newLookupErrorResponse(ServerError.TooManyRequests, "too many", requestId));
            } else if (error == LookupError.TIMEOUT) {
                // do nothing
            } else if (error == null || error == LookupError.OK) {
                super.handleLookup(lookup);
            }
        }
    }
}

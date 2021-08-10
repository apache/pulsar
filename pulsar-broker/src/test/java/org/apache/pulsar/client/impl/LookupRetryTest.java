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

import java.util.concurrent.ArrayBlockingQueue;
import static org.apache.pulsar.common.protocol.Commands.newLookupErrorResponse;
import static org.apache.pulsar.common.protocol.Commands.newPartitionMetadataResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import com.google.common.collect.Sets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.PulsarChannelInitializer;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.naming.TopicName;

public class LookupRetryTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(LookupRetryTest.class);

    private static final String subscription = "reader-sub";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));
    }

    @Override
    protected PulsarService newPulsarService(ServiceConfiguration conf) throws Exception {
        return new PulsarService(conf) {
            @Override
            protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
                BrokerService broker = new BrokerService(this, ioEventLoopGroup);
                broker.setPulsarChannelInitializerFactory(
                        (_pulsar, tls) -> {
                            return new PulsarChannelInitializer(_pulsar, tls) {
                                @Override
                                protected ServerCnx newServerCnx(PulsarService pulsar) throws Exception {
                                    return new ErrorByTopicServerCnx(pulsar);
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

    enum LookupError {
        UNKNOWN,
        TOO_MANY,
        TIMEOUT,
        OK,
    }

    private static class ErrorByTopicServerCnx extends ServerCnx {
        private ConcurrentHashMap<String, Queue<LookupError>> failureMap = new ConcurrentHashMap<>();

        ErrorByTopicServerCnx(PulsarService pulsar) {
            super(pulsar);
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

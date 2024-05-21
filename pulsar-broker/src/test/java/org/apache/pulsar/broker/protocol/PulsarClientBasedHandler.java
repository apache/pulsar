/*
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
package org.apache.pulsar.broker.protocol;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;

public class PulsarClientBasedHandler implements ProtocolHandler {

    static final String PROTOCOL = "test";

    private String topic;
    private int partitions;
    private String cluster;
    private PulsarClient client;
    private List<Reader<byte[]>> readers;
    private ExecutorService executor;
    private volatile boolean running = false;
    volatile long closeTimeMs;

    @Override
    public String protocolName() {
        return PROTOCOL;
    }

    @Override
    public boolean accept(String protocol) {
        return protocol.equals(PROTOCOL);
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        final var properties = conf.getProperties();
        topic = (String) properties.getOrDefault("metadata.topic", "metadata-topic");
        partitions = (Integer) properties.getOrDefault("metadata.partitions", 1);
        cluster = conf.getClusterName();
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return "";
    }

    @Override
    public void start(BrokerService service) {
        try {
            final var port = service.getPulsar().getListenPortHTTP().orElseThrow();
            @Cleanup final var admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + port).build();
            try {
                admin.clusters().createCluster(cluster, ClusterData.builder()
                        .serviceUrl(service.getPulsar().getWebServiceAddress())
                        .serviceUrlTls(service.getPulsar().getWebServiceAddressTls())
                        .brokerServiceUrl(service.getPulsar().getBrokerServiceUrl())
                        .brokerServiceUrlTls(service.getPulsar().getBrokerServiceUrlTls())
                        .build());
            } catch (PulsarAdminException ignored) {
            }
            try {
                admin.tenants().createTenant("public", TenantInfo.builder().allowedClusters(Set.of(cluster)).build());
            } catch (PulsarAdminException ignored) {
            }
            try {
                admin.namespaces().createNamespace("public/default");
            } catch (PulsarAdminException ignored) {
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        try {
            final var port = service.getListenPort().orElseThrow();
            client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + port).build();
            readers = new ArrayList<>();
            for (int i = 0; i < partitions; i++) {
                readers.add(client.newReader().topic(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + i)
                        .startMessageId(MessageId.earliest).create());
            }
            running = true;
            executor = Executors.newSingleThreadExecutor();
            executor.execute(() -> {
                while (running) {
                    readers.forEach(reader -> {
                        try {
                            reader.readNext(1, TimeUnit.MILLISECONDS);
                        } catch (PulsarClientException ignored) {
                        }
                    });
                }
            });
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        return Map.of();
    }

    @Override
    public void close() {
        final var start = System.currentTimeMillis();
        running = false;
        if (client != null) {
            try {
                client.close();
            } catch (PulsarClientException ignored) {
            }
            client = null;
        }
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
        closeTimeMs = System.currentTimeMillis() - start;
    }
}

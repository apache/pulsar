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
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.proxy.stats.ConnectionStats;
import org.apache.pulsar.proxy.stats.TopicStats;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyStatsTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private WebServer proxyWebServer;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(GLOBAL_DUMMY_VALUE);
        // enable full parsing feature
        proxyConfig.setProxyLogLevel(Optional.of(2));

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        Optional<Integer> proxyLogLevel = Optional.of(2);
        assertEquals(proxyLogLevel, proxyService.getConfiguration().getProxyLogLevel());
        proxyService.start();

        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));

        proxyWebServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(proxyWebServer, proxyConfig, proxyService, null);
        proxyWebServer.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        proxyService.close();
    }

    /**
     * Validates proxy connection stats api.
     *
     * @throws Exception
     */
    @Test
    public void testConnectionsStats() throws Exception {
        final String topicName1 = "persistent://sample/test/local/connections-stats";
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl()).build();
        Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(topicName1).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName1).subscriptionName("my-sub").subscribe();

        int totalMessages = 10;
        for (int i = 0; i < totalMessages; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < totalMessages; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
            consumer.acknowledge(msg);
        }

        Client httpClient = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
        Response r = httpClient.target(proxyWebServer.getServiceUri()).path("/proxy-stats/connections").request()
                .get();
        Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
        String response = r.readEntity(String.class).trim();
        List<ConnectionStats> connectionStats = new Gson().fromJson(response, new TypeToken<List<ConnectionStats>>() {
        }.getType());

        assertNotNull(connectionStats);

        consumer.close();
    }

    /**
     * Validate proxy topic stats api
     *
     * @throws Exception
     */
    @Test
    public void testTopicStats() throws Exception {
        proxyService.setProxyLogLevel(2);
        final String topicName = "persistent://sample/test/local/topic-stats";
        final String topicName2 = "persistent://sample/test/local/topic-stats-2";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl()).build();
        Producer<byte[]> producer1 = client.newProducer(Schema.BYTES).topic(topicName).enableBatching(false)
                .producerName("producer1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        Producer<byte[]> producer2 = client.newProducer(Schema.BYTES).topic(topicName2).enableBatching(false)
                .producerName("producer2").messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topicName2).subscriptionName("my-sub")
                .subscribe();

        int totalMessages = 10;
        for (int i = 0; i < totalMessages; i++) {
            producer1.send("test".getBytes());
            producer2.send("test".getBytes());
        }

        for (int i = 0; i < totalMessages; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
            consumer.acknowledge(msg);
            msg = consumer2.receive(1, TimeUnit.SECONDS);
        }

        Client httpClient = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
        Response r = httpClient.target(proxyWebServer.getServiceUri()).path("/proxy-stats/topics").request()
                .get();
        Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
        String response = r.readEntity(String.class).trim();
        Map<String, TopicStats> topicStats = new Gson().fromJson(response, new TypeToken<Map<String, TopicStats>>() {
        }.getType());

        assertNotNull(topicStats.get(topicName));

        consumer.close();
        consumer2.close();
    }

    /**
     * Change proxy log level dynamically
     *
     * @throws Exception
     */
    @Test
    public void testChangeLogLevel() {
        Assert.assertEquals(proxyService.getProxyLogLevel(), 2);
        int newLogLevel = 1;
        Client httpClient = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
        Response r = httpClient.target(proxyWebServer.getServiceUri()).path("/proxy-stats/logging/" + newLogLevel)
                .request().post(Entity.entity("", MediaType.APPLICATION_JSON));
        Assert.assertEquals(r.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        Assert.assertEquals(proxyService.getProxyLogLevel(), newLogLevel);
    }

}

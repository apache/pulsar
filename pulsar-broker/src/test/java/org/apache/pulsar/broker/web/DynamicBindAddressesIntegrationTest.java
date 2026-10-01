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
package org.apache.pulsar.broker.web;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.net.InetAddress;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration test for configuring the broker entirely through {@code bindAddresses} with dynamic
 * ports (port {@code 0}). Verifies that:
 * <ul>
 *   <li>None of the legacy {@code brokerServicePort}/{@code webServicePort} (and TLS variants)
 *       need to be set when {@code bindAddresses} supplies {@code pulsar://} and {@code http://}
 *       entries under the internal listener;</li>
 *   <li>after the broker binds, the actual bound ports are reflected back into the configuration
 *       and the internal advertised listener is synthesized with the real ports;</li>
 *   <li>{@code advertisedAddress} defaults to the local hostname when left blank;</li>
 *   <li>{@code PulsarAdmin} and {@code PulsarClient} can talk to the broker via the resolved
 *       service URLs.</li>
 * </ul>
 *
 * <p>This exercises the post-bind dependency: only after the Netty/Jetty servers have bound is
 * the dynamic port known, and only then can the advertised listener map be populated.
 */
@Test(groups = "broker")
public class DynamicBindAddressesIntegrationTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        // Provision the standard `public/default` namespace that the admin/client tests below use.
        // This cannot be done from inside the constructor flow because the admin client only becomes
        // reachable after the dynamic web port has been bound and surfaced into the configuration.
        admin.clusters().createCluster(conf.getClusterName(),
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                TenantInfo.builder()
                        .allowedClusters(Set.of(conf.getClusterName()))
                        .build());
        admin.namespaces().createNamespace("public/default");
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // Clear every legacy port property — the broker must come up purely from `bindAddresses`.
        conf.setBrokerServicePort(Optional.empty());
        conf.setBrokerServicePortTls(Optional.empty());
        conf.setWebServicePort(Optional.empty());
        conf.setWebServicePortTls(Optional.empty());
        // Bind to all interfaces on a dynamically assigned port for both schemes, tagged with the
        // internal listener so the connectors are selected as the primary ones for broker-to-broker
        // communication.
        conf.setBindAddresses("internal:pulsar://0.0.0.0:0,internal:http://0.0.0.0:0");
        // Force `advertisedAddress` to be blank so we exercise the hostname-fallback path.
        conf.setAdvertisedAddress(null);
        // No explicit `advertisedListeners` either; the internal listener must be synthesized from
        // the dynamically bound ports.
        conf.setAdvertisedListeners(null);
    }

    @Test
    public void testRuntimeListenerSynthesizedFromDynamicBindAddresses() {
        ServiceConfiguration runtimeConf = pulsar.getConfiguration();

        // After binding, the legacy port properties must reflect the actual dynamic ports.
        assertTrue(runtimeConf.getBrokerServicePort().isPresent(),
                "brokerServicePort should be populated from the bound port");
        assertTrue(runtimeConf.getWebServicePort().isPresent(),
                "webServicePort should be populated from the bound port");
        int boundPulsarPort = runtimeConf.getBrokerServicePort().get();
        int boundWebPort = runtimeConf.getWebServicePort().get();
        assertFalse(boundPulsarPort == 0, "the dynamic pulsar port must not still be 0");
        assertFalse(boundWebPort == 0, "the dynamic web port must not still be 0");
        // TLS variants stay empty because no TLS binding was declared.
        assertFalse(runtimeConf.getBrokerServicePortTls().isPresent());
        assertFalse(runtimeConf.getWebServicePortTls().isPresent());

        // The advertised address must default to the local hostname.
        String expectedHost;
        try {
            expectedHost = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            throw new AssertionError("unable to resolve canonical local hostname for the test", e);
        }

        // The internal listener must now exist in the cached map with both URLs populated.
        Map<String, AdvertisedListener> listeners = pulsar.getAdvertisedListeners();
        assertNotNull(listeners, "advertised listener map must not be null");
        AdvertisedListener internal = listeners.get(ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME);
        assertNotNull(internal,
                "the `internal` listener must have been synthesized after binding, but the map was: "
                        + listeners);
        assertEquals(internal.getBrokerServiceUrl(),
                URI.create("pulsar://" + expectedHost + ":" + boundPulsarPort));
        assertEquals(internal.getBrokerHttpUrl(),
                URI.create("http://" + expectedHost + ":" + boundWebPort));

        // The top-level service URLs must agree with the listener.
        assertEquals(pulsar.getBrokerServiceUrl(), "pulsar://" + expectedHost + ":" + boundPulsarPort);
        assertEquals(pulsar.getWebServiceAddress(), "http://" + expectedHost + ":" + boundWebPort);

        // The broker id is derived from the (post-bind) advertised address and web port.
        String brokerId = pulsar.getBrokerId();
        assertNotNull(brokerId, "brokerId must be initialized after start()");
        assertEquals(brokerId, expectedHost + ":" + boundWebPort);
    }

    @Test
    public void testPulsarAdminCanCreateTopicOverDynamicBindAddress() throws Exception {
        String namespace = "public/default";
        String topic = "persistent://" + namespace + "/dynamic-bind-admin-" + UUID.randomUUID();

        // The `admin` field is built against the runtime web URL by MockedPulsarServiceBaseTest;
        // exercise it with a real round-trip to confirm the URL is reachable.
        try (PulsarAdmin localAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getWebServiceAddress())
                .build()) {
            localAdmin.topics().createNonPartitionedTopic(topic);
            assertTrue(localAdmin.topics().getList(namespace).contains(topic),
                    "topic should be listed under its namespace after creation");
        }
    }

    @Test
    public void testPulsarClientProduceConsumeOverDynamicBindAddress() throws Exception {
        String topic = "persistent://public/default/dynamic-bind-pubsub-" + UUID.randomUUID();
        String subscription = "test-sub";
        String payload = "hello-dynamic-bind";

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .build()) {
            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(topic)
                    .subscriptionName(subscription)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
                 Producer<String> producer = client.newProducer(Schema.STRING)
                         .topic(topic)
                         .create()) {
                producer.send(payload);
                Message<String> received = consumer.receive(10, TimeUnit.SECONDS);
                assertNotNull(received, "consumer must receive the message produced over the dynamic port");
                assertEquals(received.getValue(), payload);
                consumer.acknowledge(received);
            }
        }
    }
}

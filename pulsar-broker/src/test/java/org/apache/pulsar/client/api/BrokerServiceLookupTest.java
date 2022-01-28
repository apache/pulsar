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
package org.apache.pulsar.client.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.naming.AuthenticationException;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceUnit;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class BrokerServiceLookupTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(BrokerServiceLookupTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setDefaultNumberOfNamespaceBundles(1);
        isTcpLookup = true;
        internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    /**
     * Usecase Multiple Broker => Lookup Redirection test
     *
     * 1. Broker1 is a leader 2. Lookup request reaches to Broker2 which redirects to leader (Broker1) with
     * authoritative = false 3. Leader (Broker1) finds out least loaded broker as Broker2 and redirects request to
     * Broker2 with authoritative = true 4. Broker2 receives final request to own a bundle with authoritative = true and
     * client connects to Broker2
     *
     * @throws Exception
     */
    @Test
    public void testMultipleBrokerLookup() throws Exception {
        log.info("-- Starting {} test --", methodName);

        /**** start broker-2 ****/
        ServiceConfiguration conf2 = new ServiceConfiguration();
        conf2.setBrokerShutdownTimeoutMs(0L);
        conf2.setBrokerServicePort(Optional.of(0));
        conf2.setWebServicePort(Optional.of(0));
        conf2.setAdvertisedAddress("localhost");
        conf2.setClusterName(conf.getClusterName());
        conf2.setZookeeperServers("localhost:2181");
        conf2.setConfigurationStoreServers("localhost:3181");

        @Cleanup
        PulsarService pulsar2 = startBroker(conf2);
        pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
        pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

        LoadManager loadManager1 = spy(pulsar.getLoadManager().get());
        LoadManager loadManager2 = spy(pulsar2.getLoadManager().get());
        Field loadManagerField = NamespaceService.class.getDeclaredField("loadManager");
        loadManagerField.setAccessible(true);

        // mock: redirect request to leader [2]
        doReturn(true).when(loadManager2).isCentralized();
        loadManagerField.set(pulsar2.getNamespaceService(), new AtomicReference<>(loadManager2));

        // mock: return Broker2 as a Least-loaded broker when leader receives request [3]
        doReturn(true).when(loadManager1).isCentralized();
        SimpleResourceUnit resourceUnit = new SimpleResourceUnit(pulsar2.getSafeWebServiceAddress(), null);
        doReturn(Optional.of(resourceUnit)).when(loadManager1).getLeastLoaded(any(ServiceUnitId.class));
        doReturn(Optional.of(resourceUnit)).when(loadManager2).getLeastLoaded(any(ServiceUnitId.class));
        loadManagerField.set(pulsar.getNamespaceService(), new AtomicReference<>(loadManager1));

        /**** started broker-2 ****/

        @Cleanup
        PulsarClient pulsarClient2 = PulsarClient.builder().serviceUrl(pulsar2.getBrokerServiceUrl()).build();

        // load namespace-bundle by calling Broker2
        Consumer<byte[]> consumer = pulsarClient2.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic("persistent://my-property/my-ns/my-topic1")
            .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        producer.close();

    }

    @Test
    public void testConcurrentWriteBrokerData() throws Exception {
        Map<String, NamespaceBundleStats> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put("key"+ i, new NamespaceBundleStats());
        }
        BrokerService brokerService = mock(BrokerService.class);
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(map).when(brokerService).getBundleStats();
        ModularLoadManagerWrapper loadManager = (ModularLoadManagerWrapper)pulsar.getLoadManager().get();

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<?>> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            LocalBrokerData data = loadManager.getLoadManager().updateLocalBrokerData();
            data.cleanDeltas();
            data.getBundles().clear();
            list.add(executor.submit(() -> {
                try {
                    assertNotNull(loadManager.generateLoadReport());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
            list.add(executor.submit(() -> {
                try {
                    loadManager.writeLoadReportOnZookeeper();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> future : list) {
            future.get();
        }
    }

    /**
     * Usecase: Redirection due to different cluster 1. Broker1 runs on cluster: "use" and Broker2 runs on cluster:
     * "use2" 2. Broker1 receives "use2" cluster request => Broker1 reads "/clusters" from global-zookeeper and
     * redirects request to Broker2 which serves "use2" 3. Broker2 receives redirect request and own namespace bundle
     *
     * @throws Exception
     */
    @Test(enabled = false) // See https://github.com/apache/pulsar/issues/5437
    public void testMultipleBrokerDifferentClusterLookup() throws Exception {
        log.info("-- Starting {} test --", methodName);

        /**** start broker-2 ****/
        final String newCluster = "use2";
        final String property = "my-property2";
        ServiceConfiguration conf2 = new ServiceConfiguration();
        conf2.setAdvertisedAddress("localhost");
        conf2.setBrokerShutdownTimeoutMs(0L);
        conf2.setBrokerServicePort(Optional.of(0));
        conf2.setWebServicePort(Optional.of(0));
        conf2.setAdvertisedAddress("localhost");
        conf2.setClusterName(newCluster); // Broker2 serves newCluster
        conf2.setZookeeperServers("localhost:2181");
        conf2.setConfigurationStoreServers("localhost:3181");
        String broker2ServiceUrl = "pulsar://localhost:" + conf2.getBrokerServicePort().get();

        admin.clusters().createCluster(newCluster,
                ClusterData.builder()
                        .serviceUrl(pulsar.getWebServiceAddress())
                        .brokerServiceUrl(broker2ServiceUrl)
                        .build());
        admin.tenants().createTenant(property,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(newCluster)));
        admin.namespaces().createNamespace(property + "/" + newCluster + "/my-ns");

        @Cleanup
        PulsarService pulsar2 = startBroker(conf2);
        pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
        pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

        URI brokerServiceUrl = new URI(broker2ServiceUrl);
        @Cleanup
        PulsarClient pulsarClient2 = PulsarClient.builder().serviceUrl(brokerServiceUrl.toString()).build();

        // enable authorization: so, broker can validate cluster and redirect if finds different cluster
        pulsar.getConfiguration().setAuthorizationEnabled(true);
        // restart broker with authorization enabled: it initialize AuthorizationService
        stopBroker();
        startBroker();

        LoadManager loadManager2 = spy(pulsar2.getLoadManager().get());
        Field loadManagerField = NamespaceService.class.getDeclaredField("loadManager");
        loadManagerField.setAccessible(true);

        // mock: return Broker2 as a Least-loaded broker when leader receives request
        doReturn(true).when(loadManager2).isCentralized();
        SimpleResourceUnit resourceUnit = new SimpleResourceUnit(pulsar2.getSafeWebServiceAddress(), null);
        doReturn(Optional.of(resourceUnit)).when(loadManager2).getLeastLoaded(any(ServiceUnitId.class));
        loadManagerField.set(pulsar.getNamespaceService(), new AtomicReference<>(loadManager2));
        /**** started broker-2 ****/

        // load namespace-bundle by calling Broker2
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property2/use2/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
        Producer<byte[]> producer = pulsarClient2.newProducer(Schema.BYTES)
            .topic("persistent://my-property2/use2/my-ns/my-topic1")
            .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        producer.close();

        // disable authorization
        pulsar.getConfiguration().setAuthorizationEnabled(false);
        loadManager2 = null;
    }

    /**
     * Create #PartitionedTopic and let it served by multiple brokers which requires a. tcp partitioned-metadata-lookup
     * b. multiple topic-lookup c. partitioned producer-consumer
     *
     * @throws Exception
     */
    @Test
    public void testPartitionTopicLookup() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numPartitions = 8;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/my-partitionedtopic1");

        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);

        /**** start broker-2 ****/
        ServiceConfiguration conf2 = new ServiceConfiguration();
        conf2.setAdvertisedAddress("localhost");
        conf2.setBrokerShutdownTimeoutMs(0L);
        conf2.setBrokerServicePort(Optional.of(0));
        conf2.setWebServicePort(Optional.of(0));
        conf2.setAdvertisedAddress("localhost");
        conf2.setClusterName(pulsar.getConfiguration().getClusterName());
        conf2.setZookeeperServers("localhost:2181");
        conf2.setConfigurationStoreServers("localhost:3181");

        @Cleanup
        PulsarService pulsar2 = startBroker(conf2);
        pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
        pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

        LoadManager loadManager1 = spy(pulsar.getLoadManager().get());
        LoadManager loadManager2 = spy(pulsar2.getLoadManager().get());
        Field loadManagerField = NamespaceService.class.getDeclaredField("loadManager");
        loadManagerField.setAccessible(true);

        // mock: return Broker2 as a Least-loaded broker when leader receives request
        doReturn(true).when(loadManager1).isCentralized();
        loadManagerField.set(pulsar.getNamespaceService(), new AtomicReference<>(loadManager1));

        // mock: redirect request to leader
        doReturn(true).when(loadManager2).isCentralized();
        loadManagerField.set(pulsar2.getNamespaceService(), new AtomicReference<>(loadManager2));
        /**** broker-2 started ****/

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscribe();

        for (int i = 0; i < 20; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 20; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg, "Message should not be null");
            consumer.acknowledge(msg);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            assertTrue(messageSet.add(receivedMessage), "Message " + receivedMessage + " already received");
        }

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        admin.topics().deletePartitionedTopic(topicName.toString());

        loadManager2 = null;

        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * 1. Start broker1 and broker2 with tls enable 2. Hit HTTPS lookup url at broker2 which redirects to HTTPS broker1
     *
     * @throws Exception
     */
    @Test
    public void testWebserviceServiceTls() throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
        final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
        final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/certificate/client.crt";
        final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/certificate/client.key";

        /**** start broker-2 ****/
        ServiceConfiguration conf2 = new ServiceConfiguration();
        conf2.setBrokerShutdownTimeoutMs(0L);
        conf2.setAdvertisedAddress("localhost");
        conf2.setBrokerShutdownTimeoutMs(0L);
        conf2.setBrokerServicePort(Optional.of(0));
        conf2.setBrokerServicePortTls(Optional.of(0));
        conf2.setWebServicePort(Optional.of(0));
        conf2.setWebServicePortTls(Optional.of(0));
        conf2.setAdvertisedAddress("localhost");
        conf2.setTlsAllowInsecureConnection(true);
        conf2.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf2.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf2.setClusterName(conf.getClusterName());
        conf2.setZookeeperServers("localhost:2181");
        conf2.setConfigurationStoreServers("localhost:3181");

        @Cleanup
        PulsarService pulsar2 = startBroker(conf2);

        // restart broker1 with tls enabled
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsAllowInsecureConnection(true);
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);
        stopBroker();
        startBroker();
        pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
        pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

        LoadManager loadManager1 = spy(pulsar.getLoadManager().get());
        LoadManager loadManager2 = spy(pulsar2.getLoadManager().get());
        Field loadManagerField = NamespaceService.class.getDeclaredField("loadManager");
        loadManagerField.setAccessible(true);

        // mock: redirect request to leader [2]
        doReturn(true).when(loadManager2).isCentralized();
        loadManagerField.set(pulsar2.getNamespaceService(), new AtomicReference<>(loadManager2));
        loadManagerField.set(pulsar.getNamespaceService(), new AtomicReference<>(loadManager1));

        // mock: return Broker2 as a Least-loaded broker when leader receives
        // request [3]
        doReturn(true).when(loadManager1).isCentralized();
        doReturn(true).when(loadManager2).isCentralized();
        SimpleResourceUnit resourceUnit = new SimpleResourceUnit(pulsar.getWebServiceAddress(), null);
        doReturn(Optional.of(resourceUnit)).when(loadManager2).getLeastLoaded(any(ServiceUnitId.class));
        doReturn(Optional.of(resourceUnit)).when(loadManager1).getLeastLoaded(any(ServiceUnitId.class));


        /**** started broker-2 ****/

        URI brokerServiceUrl = new URI("pulsar://localhost:" + conf2.getBrokerServicePort().get());
        @Cleanup
        PulsarClient pulsarClient2 = PulsarClient.builder().serviceUrl(brokerServiceUrl.toString()).build();

        final String lookupResourceUrl = "/lookup/v2/topic/persistent/my-property/my-ns/my-topic1";

        // set client cert_key file
        KeyManager[] keyManagers = null;
        Certificate[] tlsCert = SecurityUtility.loadCertificatesFromPemFile(TLS_CLIENT_CERT_FILE_PATH);
        PrivateKey tlsKey = SecurityUtility.loadPrivateKeyFromPemFile(TLS_CLIENT_KEY_FILE_PATH);
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        ks.setKeyEntry("private", tlsKey, "".toCharArray(), tlsCert);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, "".toCharArray());
        keyManagers = kmf.getKeyManagers();
        TrustManager[] trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(keyManagers, trustManagers, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sslCtx.getSocketFactory());

        // hit broker2 url
        URLConnection con = new URL(pulsar2.getWebServiceAddressTls() + lookupResourceUrl).openConnection();
        log.info("orignal url: {}", con.getURL());
        con.connect();
        log.info("connected url: {} ", con.getURL());
        // assert connect-url: broker2-https
        assertEquals(new Integer(con.getURL().getPort()), conf2.getWebServicePortTls().get());
        InputStream is = con.getInputStream();
        // assert redirect-url: broker1-https only
        log.info("redirected url: {}", con.getURL());
        assertEquals(new Integer(con.getURL().getPort()), conf.getWebServicePortTls().get());
        is.close();

        loadManager1 = null;
        loadManager2 = null;
    }

    /**
     *
     * <pre>
     * When broker-1's load-manager splits the bundle and update local-policies, broker-2 should get watch of
     * local-policies and update bundleCache so, new lookup can be redirected properly.
     *
     * (1) Start broker-1 and broker-2
     * (2) Make sure broker-2 always assign bundle to broker1
     * (3) Broker-2 receives topic-1 request, creates local-policies and sets the watch
     * (4) Broker-1 will own topic-1
     * (5) Split the bundle for topic-1
     * (6) Broker-2 should get the watch and update bundle cache
     * (7) Make lookup request again to Broker-2 which should succeed.
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test(timeOut = 5000)
    public void testSplitUnloadLookupTest() throws Exception {

        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/my-ns";
        // (1) Start broker-1
        ServiceConfiguration conf2 = new ServiceConfiguration();
        conf2.setAdvertisedAddress("localhost");
        conf2.setBrokerShutdownTimeoutMs(0L);
        conf2.setBrokerServicePort(Optional.of(0));
        conf2.setWebServicePort(Optional.of(0));
        conf2.setAdvertisedAddress("localhost");
        conf2.setClusterName(conf.getClusterName());
        conf2.setZookeeperServers("localhost:2181");
        conf2.setConfigurationStoreServers("localhost:3181");

        @Cleanup
        PulsarService pulsar2 = startBroker(conf2);
        pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
        pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

        pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
        pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

        LoadManager loadManager1 = spy(pulsar.getLoadManager().get());
        LoadManager loadManager2 = spy(pulsar2.getLoadManager().get());
        Field loadManagerField = NamespaceService.class.getDeclaredField("loadManager");
        loadManagerField.setAccessible(true);

        // (2) Make sure broker-2 always assign bundle to broker1
        // mock: redirect request to leader [2]
        doReturn(true).when(loadManager2).isCentralized();
        loadManagerField.set(pulsar2.getNamespaceService(), new AtomicReference<>(loadManager2));
        // mock: return Broker1 as a Least-loaded broker when leader receives request [3]
        doReturn(true).when(loadManager1).isCentralized();
        SimpleResourceUnit resourceUnit = new SimpleResourceUnit(pulsar.getSafeWebServiceAddress(), null);
        doReturn(Optional.of(resourceUnit)).when(loadManager1).getLeastLoaded(any(ServiceUnitId.class));
        doReturn(Optional.of(resourceUnit)).when(loadManager2).getLeastLoaded(any(ServiceUnitId.class));
        loadManagerField.set(pulsar.getNamespaceService(), new AtomicReference<>(loadManager1));

        @Cleanup
        PulsarClient pulsarClient2 = PulsarClient.builder().serviceUrl(pulsar2.getBrokerServiceUrl()).build();

        // (3) Broker-2 receives topic-1 request, creates local-policies and sets the watch
        final String topic1 = "persistent://" + namespace + "/topic1";
        Consumer<byte[]> consumer1 = pulsarClient2.newConsumer().topic(topic1).subscriptionName("my-subscriber-name")
                .subscribe();

        Set<String> serviceUnits1 = pulsar.getNamespaceService().getOwnedServiceUnits().stream()
                .map(nb -> nb.toString()).collect(Collectors.toSet());

        // (4) Broker-1 will own topic-1
        final String unsplitBundle = namespace + "/0x00000000_0xffffffff";
        assertTrue(serviceUnits1.contains(unsplitBundle));
        // broker-2 should have this bundle into the cache
        TopicName topicName = TopicName.get(topic1);
        NamespaceBundle bundleInBroker2 = pulsar2.getNamespaceService().getBundle(topicName);
        assertEquals(bundleInBroker2.toString(), unsplitBundle);

        // (5) Split the bundle for topic-1
        admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", true, null);

        // (6) Broker-2 should get the watch and update bundle cache
        final int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (pulsar2.getNamespaceService().getBundle(topicName).equals(bundleInBroker2) && i != retry - 1) {
                Thread.sleep(200);
            } else {
                break;
            }
        }

        // (7) Make lookup request again to Broker-2 which should succeed.
        final String topic2 = "persistent://" + namespace + "/topic2";
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topic2).subscriptionName("my-subscriber-name")
                .subscribe();

        NamespaceBundle bundleInBroker1AfterSplit = pulsar2.getNamespaceService().getBundle(TopicName.get(topic2));
        assertNotEquals(unsplitBundle, bundleInBroker1AfterSplit);

        consumer1.close();
        consumer2.close();
    }

    /**
     *
     * <pre>
     * When broker-1's Modular-load-manager splits the bundle and update local-policies, broker-2 should get watch of
     * local-policies and update bundleCache so, new lookup can be redirected properly.
     *
     * (1) Start broker-1 and broker-2
     * (2) Make sure broker-2 always assign bundle to broker1
     * (3) Broker-2 receives topic-1 request, creates local-policies and sets the watch
     * (4) Broker-1 will own topic-1
     * (5) Broker-2 will be a leader and trigger Split the bundle for topic-1
     * (6) Broker-2 should get the watch and update bundle cache
     * (7) Make lookup request again to Broker-2 which should succeed.
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test(timeOut = 20000)
    public void testModularLoadManagerSplitBundle() throws Exception {

        log.info("-- Starting {} test --", methodName);
        final String loadBalancerName = conf.getLoadManagerClassName();

        try {
            final String namespace = "my-property/my-ns";
            // (1) Start broker-1
            ServiceConfiguration conf2 = new ServiceConfiguration();
            conf2.setBrokerShutdownTimeoutMs(0L);
            conf2.setAdvertisedAddress("localhost");
            conf2.setBrokerShutdownTimeoutMs(0L);
            conf2.setBrokerServicePort(Optional.of(0));
            conf2.setWebServicePort(Optional.of(0));
            conf2.setAdvertisedAddress("localhost");
            conf2.setClusterName(conf.getClusterName());
            conf2.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
            conf2.setZookeeperServers("localhost:2181");
            conf2.setConfigurationStoreServers("localhost:3181");
            conf2.setLoadBalancerAutoBundleSplitEnabled(true);
            conf2.setLoadBalancerAutoUnloadSplitBundlesEnabled(true);
            conf2.setLoadBalancerNamespaceBundleMaxTopics(1);

            // configure broker-1 with ModularLoadManager
            stopBroker();
            conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
            startBroker();

            @Cleanup
            PulsarService pulsar2 = startBroker(conf2);

            pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
            pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

            LoadManager loadManager1 = spy(pulsar.getLoadManager().get());
            LoadManager loadManager2 = spy(pulsar2.getLoadManager().get());
            Field loadManagerField = NamespaceService.class.getDeclaredField("loadManager");
            loadManagerField.setAccessible(true);

            // (2) Make sure broker-2 always assign bundle to broker1
            // mock: redirect request to leader [2]
            doReturn(true).when(loadManager2).isCentralized();
            loadManagerField.set(pulsar2.getNamespaceService(), new AtomicReference<>(loadManager2));
            // mock: return Broker1 as a Least-loaded broker when leader receives request [3]
            doReturn(true).when(loadManager1).isCentralized();
            SimpleResourceUnit resourceUnit = new SimpleResourceUnit(pulsar.getSafeWebServiceAddress(), null);
            Optional<ResourceUnit> res = Optional.of(resourceUnit);
            doReturn(res).when(loadManager1).getLeastLoaded(any(ServiceUnitId.class));
            doReturn(res).when(loadManager2).getLeastLoaded(any(ServiceUnitId.class));
            loadManagerField.set(pulsar.getNamespaceService(), new AtomicReference<>(loadManager1));

            @Cleanup
            PulsarClient pulsarClient2 = PulsarClient.builder().serviceUrl(pulsar2.getBrokerServiceUrl()).build();

            // (3) Broker-2 receives topic-1 request, creates local-policies and sets the watch
            final String topic1 = "persistent://" + namespace + "/topic1";
            @Cleanup
            Consumer<byte[]> consumer1 = pulsarClient2.newConsumer().topic(topic1)
                    .subscriptionName("my-subscriber-name").subscribe();

            // there should be more than one topic to trigger split
            final String topic2 = "persistent://" + namespace + "/topic2";
            @Cleanup
            Consumer<byte[]> consumer2 = pulsarClient2.newConsumer().topic(topic2)
                    .subscriptionName("my-subscriber-name")
                    .subscribe();

            // (4) Broker-1 will own topic-1
            final String unsplitBundle = namespace + "/0x00000000_0xffffffff";

            Awaitility.await().until(() ->
                    pulsar.getNamespaceService().getOwnedServiceUnits()
                    .stream()
                    .map(nb -> nb.toString())
                    .collect(Collectors.toSet())
                    .contains(unsplitBundle));

            // broker-2 should have this bundle into the cache
            TopicName topicName = TopicName.get(topic1);
            NamespaceBundle bundleInBroker2 = pulsar2.getNamespaceService().getBundle(topicName);
            assertEquals(bundleInBroker2.toString(), unsplitBundle);

            // update broker-1 bundle report to zk
            pulsar.getBrokerService().updateRates();
            pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
            // this will create znode for bundle-data
            pulsar.getLoadManager().get().writeResourceQuotasToZooKeeper();
            pulsar2.getLoadManager().get().writeLoadReportOnZookeeper();

            // (5) Modular-load-manager will split the bundle due to max-topic threshold reached
            Method updateAllMethod = ModularLoadManagerImpl.class.getDeclaredMethod("updateAll");
            updateAllMethod.setAccessible(true);

            // broker-2 loadManager is a leader and let it refresh load-report from all the brokers
            pulsar.getLeaderElectionService().close();

            ModularLoadManagerImpl loadManager = (ModularLoadManagerImpl) ((ModularLoadManagerWrapper) pulsar2
                    .getLoadManager().get()).getLoadManager();

            updateAllMethod.invoke(loadManager);
            loadManager.checkNamespaceBundleSplit();

            // (6) Broker-2 should get the watch and update bundle cache
            Awaitility.await().untilAsserted(() -> {
                assertNotEquals(pulsar2.getNamespaceService().getBundle(topicName), bundleInBroker2);
            });

            // (7) Make lookup request again to Broker-2 which should succeed.
            final String topic3 = "persistent://" + namespace + "/topic3";
            @Cleanup
            Consumer<byte[]> consumer3 = pulsarClient2.newConsumer().topic(topic3)
                    .subscriptionName("my-subscriber-name")
                    .subscribe();

            Awaitility.await().untilAsserted(() -> {
                NamespaceBundle bundleInBroker1AfterSplit = pulsar2.getNamespaceService()
                        .getBundle(TopicName.get(topic3));
                assertNotEquals(bundleInBroker1AfterSplit.toString(), unsplitBundle);
            });
        } finally {
            conf.setLoadManagerClassName(loadBalancerName);
        }
    }

    @Test(timeOut = 10000)
    public void testPartitionedMetadataWithDeprecatedVersion() throws Exception {

        final String cluster = "use2";
        final String property = "my-property2";
        final String namespace = "my-ns";
        final String topicName = "my-partitioned";
        final int totalPartitions = 10;
        final TopicName dest = TopicName.get("persistent", property, cluster, namespace, topicName);
        admin.clusters().createCluster(cluster,
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(property,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(cluster)));
        admin.namespaces().createNamespace(property + "/" + cluster + "/" + namespace);
        admin.topics().createPartitionedTopic(dest.toString(), totalPartitions);

        stopBroker();
        conf.setClientLibraryVersionCheckEnabled(true);
        startBroker();

        URI brokerServiceUrl = new URI(pulsar.getSafeWebServiceAddress());

        URL url = brokerServiceUrl.toURL();
        String path = String.format("admin/%s/partitions", dest.getLookupName());

        AsyncHttpClient httpClient = getHttpClient("Pulsar-Java-1.20");
        PartitionedTopicMetadata metadata = getPartitionedMetadata(httpClient, url, path);
        assertEquals(metadata.partitions, totalPartitions);
        httpClient.close();

        httpClient = getHttpClient("Pulsar-CPP-v1.21");
        metadata = getPartitionedMetadata(httpClient, url, path);
        assertEquals(metadata.partitions, totalPartitions);
        httpClient.close();

        httpClient = getHttpClient("Pulsar-CPP-v1.21-SNAPSHOT");
        metadata = getPartitionedMetadata(httpClient, url, path);
        assertEquals(metadata.partitions, totalPartitions);
        httpClient.close();

        httpClient = getHttpClient("");
        try {
            metadata = getPartitionedMetadata(httpClient, url, path);
            fail("should have failed due to invalid version");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
        }
        httpClient.close();

        httpClient = getHttpClient("Pulsar-CPP-v1.20-SNAPSHOT");
        try {
            metadata = getPartitionedMetadata(httpClient, url, path);
            fail("should have failed due to invalid version");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
        }
        httpClient.close();

        httpClient = getHttpClient("Pulsar-CPP-v1.20");
        try {
            metadata = getPartitionedMetadata(httpClient, url, path);
            fail("should have failed due to invalid version");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
        }
        httpClient.close();
    }

    private PartitionedTopicMetadata getPartitionedMetadata(AsyncHttpClient httpClient, URL url, String path)
            throws Exception {
        final CompletableFuture<PartitionedTopicMetadata> future = new CompletableFuture<>();
        try {

            String requestUrl = new URL(url, path).toString();
            BoundRequestBuilder builder = httpClient.prepareGet(requestUrl);

            final ListenableFuture<Response> responseFuture = builder.setHeader("Accept", "application/json")
                    .execute(new AsyncCompletionHandler<Response>() {

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            return response;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                            future.completeExceptionally(new PulsarClientException(t));
                        }
                    });

            responseFuture.addListener(() -> {
                try {
                    Response response = responseFuture.get();
                    if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        log.warn("[{}] HTTP get request failed: {}", requestUrl, response.getStatusText());
                        future.completeExceptionally(
                                new PulsarClientException("HTTP get request failed: " + response.getStatusText()));
                        return;
                    }

                    PartitionedTopicMetadata data = ObjectMapperFactory.getThreadLocal()
                            .readValue(response.getResponseBodyAsBytes(), PartitionedTopicMetadata.class);
                    future.complete(data);
                } catch (Exception e) {
                    log.warn("[{}] Error during HTTP get request: {}", requestUrl, e.getMessage());
                    future.completeExceptionally(new PulsarClientException(e));
                }
            }, MoreExecutors.directExecutor());

        } catch (Exception e) {
            log.warn("[{}] Failed to get authentication data for lookup: {}", path, e.getMessage());
            if (e instanceof PulsarClientException) {
                future.completeExceptionally(e);
            } else {
                future.completeExceptionally(new PulsarClientException(e));
            }
        }
        return future.get();
    }

    private AsyncHttpClient getHttpClient(String version) {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setFollowRedirect(true);
        confBuilder.setUserAgent(version);
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(InetSocketAddress remoteAddress, Request ahcRequest,
                                     HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5)
                       && super.keepAlive(remoteAddress, ahcRequest, request, response);
            }
        });
        AsyncHttpClientConfig config = confBuilder.build();
        return new DefaultAsyncHttpClient(config);
    }

    /**** helper classes ****/

    public static class MockAuthenticationProvider implements AuthenticationProvider {
        @Override
        public void close() throws IOException {
        }

        @Override
        public void initialize(ServiceConfiguration config) throws IOException {
        }

        @Override
        public String getAuthMethodName() {
            return "auth";
        }

        @Override
        public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
            return "appid1";
        }
    }

    public static class MockAuthenticationProviderFail extends MockAuthenticationProvider {
        @Override
        public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
            throw new AuthenticationException("authentication failed");
        }
    }

    public static class MockAuthorizationProviderFail extends MockAuthenticationProvider {
        @Override
        public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
            return "invalid";
        }
    }
}

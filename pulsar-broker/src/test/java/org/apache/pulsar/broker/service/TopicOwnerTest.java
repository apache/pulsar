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
package org.apache.pulsar.broker.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.spy;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.Sets;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.jute.Record;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.OwnedBundle;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.namespace.ServiceUnitZkUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

public class TopicOwnerTest {

    private static final Logger log = LoggerFactory.getLogger(TopicOwnerTest.class);

    LocalBookkeeperEnsemble bkEnsemble;
    protected PulsarAdmin[] pulsarAdmins = new PulsarAdmin[BROKER_COUNT];
    protected static final int BROKER_COUNT = 5;
    protected ServiceConfiguration[] configurations = new ServiceConfiguration[BROKER_COUNT];
    protected PulsarService[] pulsarServices = new PulsarService[BROKER_COUNT];
    protected PulsarService leaderPulsar;
    protected PulsarAdmin leaderAdmin;

    @BeforeMethod
    void setup() throws Exception {
        log.info("---- Initializing TopicOwnerTest -----");
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // start brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setBrokerServicePort(Optional.of(0));
            config.setClusterName("my-cluster");
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            configurations[i] = config;

            pulsarServices[i] = new PulsarService(config);
            pulsarServices[i].start();

            // Sleep until pulsarServices[0] becomes leader, this way we can spy namespace bundle assignment easily.
            while (i == 0 && !pulsarServices[0].getLeaderElectionService().isLeader()) {
                Thread.sleep(10);
            }

            pulsarAdmins[i] = PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarServices[i].getWebServiceAddress())
                    .build();
        }
        leaderPulsar = pulsarServices[0];
        leaderAdmin = pulsarAdmins[0];
        Thread.sleep(1000);
    }

    @AfterMethod
    void tearDown() throws Exception {
        for (int i = 0; i < BROKER_COUNT; i++) {
            pulsarServices[i].close();
            pulsarAdmins[i].close();
        }
        bkEnsemble.stop();
    }

    @SuppressWarnings("unchecked")
    private MutableObject<PulsarService> spyLeaderNamespaceServiceForAuthorizedBroker() {
        // Spy leader namespace service to inject authorized broker for namespace-bundle from leader,
        // this is a safe operation since it is just an recommendation if namespace-bundle has no owner
        // currently. Namespace-bundle ownership contention is an atomic operation through zookeeper.
        NamespaceService leaderNamespaceService = leaderPulsar.getNamespaceService();
        NamespaceService spyLeaderNamespaceService = spy(leaderNamespaceService);
        final MutableObject<PulsarService> leaderAuthorizedBroker = new MutableObject<>();
        Answer<CompletableFuture<Optional<LookupResult>>> answer = invocation -> {
            PulsarService pulsarService = leaderAuthorizedBroker.getValue();
            if (pulsarService == null) {
                return (CompletableFuture<Optional<LookupResult>>) invocation.callRealMethod();
            }
            LookupResult lookupResult = new LookupResult(
                    pulsarService.getWebServiceAddress(),
                    pulsarService.getWebServiceAddressTls(),
                    pulsarService.getBrokerServiceUrl(),
                    pulsarService.getBrokerServiceUrlTls(),
                    true);
            return CompletableFuture.completedFuture(Optional.of(lookupResult));
        };
        doAnswer(answer).when(spyLeaderNamespaceService).getBrokerServiceUrlAsync(any(TopicName.class), any(LookupOptions.class));
        Whitebox.setInternalState(leaderPulsar, "nsService", spyLeaderNamespaceService);
        return leaderAuthorizedBroker;
    }

    private CompletableFuture<Void> watchZookeeperReconnect(ZooKeeper zooKeeper) throws Exception {
        CompletableFuture<Void> reconnectedFuture = new CompletableFuture<>();
        zooKeeper.exists("/", (WatchedEvent event) -> {
            Watcher.Event.KeeperState state = event.getState();
            if (state == Watcher.Event.KeeperState.SyncConnected) {
                reconnectedFuture.complete(null);
            }
        });
        return reconnectedFuture;
    }

    @FunctionalInterface
    interface RequestMatcher {
        boolean match(Request request) throws Exception;
    }

    private void spyZookeeperToDisconnectBeforePersist(ZooKeeper zooKeeper, RequestMatcher matcher) {
        ZooKeeperServer zooKeeperServer = bkEnsemble.getZkServer();
        ServerCnxn zkServerConnection = bkEnsemble.getZookeeperServerConnection(zooKeeper);
        ZooKeeperServer spyZooKeeperServer = spy(zooKeeperServer);

        // Spy zk server connection to close connection to cause connection loss after namespace-bundle
        // deleted successfully. This mimics crash of connected zk server after committing requested operation.
        Whitebox.setInternalState(zkServerConnection, "zkServer", spyZooKeeperServer);
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if (request.sessionId != zooKeeper.getSessionId()) {
                return invocation.callRealMethod();
            }
            if (!matcher.match(request)) {
                return invocation.callRealMethod();
            }
            Whitebox.setInternalState(zkServerConnection, "zkServer", zooKeeperServer);
            bkEnsemble.disconnectZookeeper(zooKeeper);
            return null;
        }).when(spyZooKeeperServer).submitRequest(any(Request.class));
    }

    private void spyZookeeperToDisconnectAfterPersist(ZooKeeper zooKeeper, RequestMatcher matcher) {
        ZooKeeperServer zooKeeperServer = bkEnsemble.getZkServer();
        ServerCnxn zkServerConnection = bkEnsemble.getZookeeperServerConnection(zooKeeper);
        ZooKeeperServer spyZooKeeperServer = spy(zooKeeperServer);

        // Spy zk server connection to close connection to cause connection loss after namespace-bundle
        // deleted successfully. This mimics crash of connected zk server after committing requested operation.
        Whitebox.setInternalState(zkServerConnection, "zkServer", spyZooKeeperServer);
        MutableBoolean disconnected = new MutableBoolean();
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if (request.sessionId != zooKeeper.getSessionId()) {
                return invocation.callRealMethod();
            }
            if (!matcher.match(request)) {
                return invocation.callRealMethod();
            }

            ServerCnxn spyZkServerConnection1 = spy(zkServerConnection);
            doAnswer(responseInvocation -> {
                synchronized (disconnected) {
                    ReplyHeader replyHeader = responseInvocation.getArgument(0);
                    if (replyHeader.getXid() == request.cxid && replyHeader.getErr() == 0) {
                        Whitebox.setInternalState(zkServerConnection, "zkServer", zooKeeperServer);
                        disconnected.setTrue();
                        bkEnsemble.disconnectZookeeper(zooKeeper);
                    } else if (disconnected.isFalse()) {
                        return responseInvocation.callRealMethod();
                    }
                    return null;
                }
            }).when(spyZkServerConnection1).sendResponse(any(ReplyHeader.class), nullable(Record.class), any(String.class));
            Whitebox.setInternalState(request, "cnxn", spyZkServerConnection1);
            return invocation.callRealMethod();
        }).when(spyZooKeeperServer).submitRequest(any(Request.class));
    }

    @Test
    public void testAcquireOwnershipWithZookeeperDisconnectedBeforeOwnershipNodeCreated() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        String topic1 = "persistent://my-tenant/my-ns/topic-1";
        NamespaceService leaderNamespaceService = leaderPulsar.getNamespaceService();
        NamespaceBundle namespaceBundle = leaderNamespaceService.getBundle(TopicName.get(topic1));

        final MutableObject<PulsarService> leaderAuthorizedBroker = spyLeaderNamespaceServiceForAuthorizedBroker();

        PulsarService pulsar1 = pulsarServices[1];
        final ZooKeeper zooKeeper1 = pulsar1.getZkClient();

        final CompletableFuture<Void> reconnectedFuture = watchZookeeperReconnect(zooKeeper1);

        String namespaceBundlePath = ServiceUnitZkUtils.path(namespaceBundle);

        spyZookeeperToDisconnectBeforePersist(zooKeeper1, request -> {
            if (request.type != ZooDefs.OpCode.create) {
                return false;
            }

            CreateRequest createRequest = new CreateRequest();
            ByteBufferInputStream.byteBuffer2Record(request.request.duplicate(), createRequest);
            return createRequest.getPath().contains(namespaceBundlePath);
        });

        leaderAuthorizedBroker.setValue(pulsar1);

        try {
            // Trigger ownership acquiring and zookeeper disconnecting before ownership node created.
            //
            // Ignore its execution result since whether it is fail or not depends on concrete implementation.
            pulsarAdmins[1].lookups().lookupTopic(topic1);
        } catch (Exception ex) {
            // Ignored intentionally.
        }

        reconnectedFuture.join();

        // We don't known whether previous lookup was successful or not, but now all lookups should succeed.
        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());

        pulsar1.getBrokerService().getTopic(topic1, true).join();

        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
    }

    @Test
    public void testAcquireOwnershipWithZookeeperDisconnectedAfterOwnershipNodeCreated() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        String topic1 = "persistent://my-tenant/my-ns/topic-1";
        NamespaceService leaderNamespaceService = leaderPulsar.getNamespaceService();
        NamespaceBundle namespaceBundle = leaderNamespaceService.getBundle(TopicName.get(topic1));

        final MutableObject<PulsarService> leaderAuthorizedBroker = spyLeaderNamespaceServiceForAuthorizedBroker();

        PulsarService pulsar1 = pulsarServices[1];
        final ZooKeeper zooKeeper1 = pulsar1.getZkClient();

        final CompletableFuture<Void> reconnectedFuture = watchZookeeperReconnect(zooKeeper1);

        String namespaceBundlePath = ServiceUnitZkUtils.path(namespaceBundle);

        spyZookeeperToDisconnectAfterPersist(zooKeeper1, request -> {
            if (request.type != ZooDefs.OpCode.create) {
                return false;
            }

            CreateRequest createRequest = new CreateRequest();
            ByteBufferInputStream.byteBuffer2Record(request.request.duplicate(), createRequest);
            return createRequest.getPath().contains(namespaceBundlePath);
        });

        leaderAuthorizedBroker.setValue(pulsar1);

        try {
            // Trigger ownership acquiring and zookeeper disconnecting after ownership node created.
            //
            // Ignore its execution result since whether it is fail or not depends on concrete implementation.
            pulsarAdmins[1].lookups().lookupTopic(topic1);
        } catch (Exception ex) {
            // Ignored intentionally.
        }

        reconnectedFuture.join();

        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());

        pulsar1.getBrokerService().getTopic(topic1, true).join();

        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
    }

    @Test
    public void testReestablishOwnershipAfterInvalidateCache() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        String topic1 = "persistent://my-tenant/my-ns/topic-1";
        NamespaceService leaderNamespaceService = leaderPulsar.getNamespaceService();
        NamespaceBundle namespaceBundle = leaderNamespaceService.getBundle(TopicName.get(topic1));

        final MutableObject<PulsarService> leaderAuthorizedBroker = spyLeaderNamespaceServiceForAuthorizedBroker();

        PulsarService pulsar1 = pulsarServices[1];

        leaderAuthorizedBroker.setValue(pulsar1);
        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());

        OwnershipCache ownershipCache1 = pulsar1.getNamespaceService().getOwnershipCache();
        AsyncLoadingCache<String, OwnedBundle> ownedBundlesCache1 = Whitebox.getInternalState(ownershipCache1, "ownedBundlesCache");

        leaderAuthorizedBroker.setValue(null);

        ownedBundlesCache1.synchronous().invalidate(ServiceUnitZkUtils.path(namespaceBundle));
        Assert.assertNull(ownershipCache1.getOwnedBundle(namespaceBundle));

        // pulsar1 is still owner in zk.
        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());

        // Reestablish ownership through lookup ownership.
        Assert.assertNull(ownershipCache1.getOwnedBundle(namespaceBundle));
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertNotNull(ownershipCache1.getOwnedBundle(namespaceBundle));

        // Reestablish ownership through check ownership.
        ownedBundlesCache1.synchronous().invalidate(ServiceUnitZkUtils.path(namespaceBundle));
        ownershipCache1.checkOwnership(namespaceBundle).join();
        Assert.assertNotNull(ownershipCache1.getOwnedBundle(namespaceBundle));

        // Reestablish ownership through load topic.
        ownedBundlesCache1.synchronous().invalidate(ServiceUnitZkUtils.path(namespaceBundle));
        pulsar1.getBrokerService().getTopic(topic1, true).join();
        Assert.assertNotNull(ownershipCache1.getOwnedBundle(namespaceBundle));
        pulsar1.getBrokerService().deleteTopic(topic1, true).join();

        // Reestablish ownership through web.
        ownedBundlesCache1.synchronous().invalidate(ServiceUnitZkUtils.path(namespaceBundle));
        pulsarAdmins[0].topics().createNonPartitionedTopic(topic1);
        Assert.assertNotNull(ownershipCache1.getOwnedBundle(namespaceBundle));
    }

    @Test
    public void testReleaseOwnershipWithZookeeperDisconnectedBeforeOwnershipNodeDeleted() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        String topic1 = "persistent://my-tenant/my-ns/topic-1";
        NamespaceService leaderNamespaceService = leaderPulsar.getNamespaceService();
        NamespaceBundle namespaceBundle = leaderNamespaceService.getBundle(TopicName.get(topic1));

        final MutableObject<PulsarService> leaderAuthorizedBroker = spyLeaderNamespaceServiceForAuthorizedBroker();

        PulsarService pulsar1 = pulsarServices[1];
        PulsarService pulsar2 = pulsarServices[2];

        leaderAuthorizedBroker.setValue(pulsar1);
        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());

        ZooKeeper zooKeeper1 = pulsar1.getZkClient();

        CompletableFuture<Void> reconnectedFuture = watchZookeeperReconnect(zooKeeper1);

        String namespaceBundlePath = ServiceUnitZkUtils.path(namespaceBundle);

        spyZookeeperToDisconnectBeforePersist(zooKeeper1, request -> {
            if (request.type != ZooDefs.OpCode.delete) {
                return false;
            }
            DeleteRequest deleteRequest = new DeleteRequest();
            ByteBufferInputStream.byteBuffer2Record(request.request.duplicate(), deleteRequest);
            return deleteRequest.getPath().contains(namespaceBundlePath);
        });

        try {
            pulsarAdmins[1].namespaces().unloadNamespaceBundle(namespaceBundle.getNamespaceObject().toString(), namespaceBundle.getBundleRange());
        } catch (Exception ex) {
            // Ignored since whether failing unloading when zk connection-loss is an implementation detail.
        }

        reconnectedFuture.join();

        leaderAuthorizedBroker.setValue(pulsar2);

        // We don't known whether previous unload was successful or not, but now all lookups should return same result.
        final String currentBrokerServiceUrl = pulsarAdmins[0].lookups().lookupTopic(topic1);
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), currentBrokerServiceUrl);
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), currentBrokerServiceUrl);
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), currentBrokerServiceUrl);
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), currentBrokerServiceUrl);

        pulsarAdmins[0].topics().createNonPartitionedTopic(topic1);
    }

    @Test
    public void testReleaseOwnershipWithZookeeperDisconnectedAfterOwnershipNodeDeleted() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        String topic1 = "persistent://my-tenant/my-ns/topic-1";
        NamespaceService leaderNamespaceService = leaderPulsar.getNamespaceService();
        NamespaceBundle namespaceBundle = leaderNamespaceService.getBundle(TopicName.get(topic1));

        final MutableObject<PulsarService> leaderAuthorizedBroker = spyLeaderNamespaceServiceForAuthorizedBroker();

        PulsarService pulsar1 = pulsarServices[1];
        PulsarService pulsar2 = pulsarServices[2];

        leaderAuthorizedBroker.setValue(pulsar1);
        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), pulsar1.getBrokerServiceUrl());

        ZooKeeper zooKeeper1 = pulsar1.getZkClient();

        CompletableFuture<Void> reconnectedFuture = watchZookeeperReconnect(zooKeeper1);

        String namespaceBundlePath = ServiceUnitZkUtils.path(namespaceBundle);

        spyZookeeperToDisconnectAfterPersist(zooKeeper1, request -> {
            if (request.type != ZooDefs.OpCode.delete) {
                return false;
            }
            DeleteRequest deleteRequest = new DeleteRequest();
            ByteBufferInputStream.byteBuffer2Record(request.request.duplicate(), deleteRequest);
            return deleteRequest.getPath().contains(namespaceBundlePath);
        });

        try {
            pulsarAdmins[1].namespaces().unloadNamespaceBundle(namespaceBundle.getNamespaceObject().toString(), namespaceBundle.getBundleRange());
        } catch (Exception ex) {
            // Ignored since whether failing unloading when zk connection-loss is an implementation detail.
        }

        reconnectedFuture.join();

        leaderAuthorizedBroker.setValue(pulsar2);

        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic1), pulsar2.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topic1), pulsar2.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topic1), pulsar2.getBrokerServiceUrl());

        pulsar2.getBrokerService().getTopic(topic1, true).join();

        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topic1), pulsar2.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topic1), pulsar2.getBrokerServiceUrl());
    }

    @Test
    public void testConnectToInvalidateBundleCacheBroker() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        Assert.assertEquals(pulsarAdmins[0].namespaces().getPolicies("my-tenant/my-ns").bundles.getNumBundles(), 16);

        final String topic1 = "persistent://my-tenant/my-ns/topic-1";
        final String topic2 = "persistent://my-tenant/my-ns/topic-2";

        // Do topic lookup here for broker to own namespace bundles
        String serviceUrlForTopic1 = pulsarAdmins[0].lookups().lookupTopic(topic1);
        String serviceUrlForTopic2 = pulsarAdmins[0].lookups().lookupTopic(topic2);

        while (serviceUrlForTopic1.equals(serviceUrlForTopic2)) {
            // Retry for bundle distribution, should make sure bundles for topic1 and topic2 are maintained in different brokers.
            pulsarAdmins[0].namespaces().unload("my-tenant/my-ns");
            serviceUrlForTopic1 = pulsarAdmins[0].lookups().lookupTopic(topic1);
            serviceUrlForTopic2 = pulsarAdmins[0].lookups().lookupTopic(topic2);
        }
        // All brokers will invalidate bundles cache after namespace bundle split
        pulsarAdmins[0].namespaces().splitNamespaceBundle("my-tenant/my-ns",
                pulsarServices[0].getNamespaceService().getBundle(TopicName.get(topic1)).getBundleRange(),
                true, null);

        PulsarClient client = PulsarClient.builder().
                serviceUrl(serviceUrlForTopic1)
                .build();

        // Check connect to a topic which owner broker invalidate all namespace bundles cache
        Consumer<byte[]> consumer = client.newConsumer().topic(topic2).subscriptionName("test").subscribe();
        Assert.assertTrue(consumer.isConnected());
    }
}

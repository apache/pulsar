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
package org.apache.pulsar.broker.service;

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.PerChannelBookieClient;
import org.apache.bookkeeper.proto.PerChannelBookieClientPool;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Test base for tests requires a bk ensemble.
 */
@Slf4j
public abstract class BkEnsemblesTestBase extends TestRetrySupport {

    protected PulsarService pulsar;
    protected ServiceConfiguration config;

    protected PulsarAdmin admin;

    protected LocalBookkeeperEnsemble bkEnsemble;

    private final int numberOfBookies;

    public BkEnsemblesTestBase() {
        this(3);
    }

    public BkEnsemblesTestBase(int numberOfBookies) {
        this.numberOfBookies = numberOfBookies;
    }

    protected void configurePulsar(ServiceConfiguration config) throws Exception {
        //overridable by subclasses
    }

    @Override
    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        incrementSetupNumber();
        try {
            // start local bookie and zookeeper
            bkEnsemble = new LocalBookkeeperEnsemble(numberOfBookies, 0, () -> 0);
            bkEnsemble.start();

            // start pulsar service
            if (config == null) {
                config = new ServiceConfiguration();
            }
            config.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setClusterName("usc");
            config.setBrokerShutdownTimeoutMs(0L);
            config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            config.setBrokerServicePort(Optional.of(0));
            config.setAuthorizationEnabled(false);
            config.setAuthenticationEnabled(false);
            config.setManagedLedgerMaxEntriesPerLedger(5);
            config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
            config.setAdvertisedAddress("127.0.0.1");
            config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
            config.setMetadataStoreOperationTimeoutSeconds(10);
            config.setNumIOThreads(1);
            Properties properties = new Properties();
            properties.put("bookkeeper_numWorkerThreads", "1");
            config.setProperties(properties);
            configurePulsar(config);

            pulsar = new PulsarService(config);
            pulsar.start();

            if (admin != null) {
                admin.close();
            }
            admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build();

            admin.clusters().createCluster("usc", ClusterData.builder()
                    .serviceUrl(pulsar.getWebServiceAddress()).build());
            admin.tenants().createTenant("prop",
                    new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("usc")));
        } catch (Throwable t) {
            log.error("Error setting up broker test", t);
            Assert.fail("Broker test setup failed");
        }
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        config = null;
        markCurrentSetupNumberCleaned();
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (pulsar != null) {
            pulsar.close();
            pulsar = null;
        }
        if (bkEnsemble != null) {
            bkEnsemble.stop();
            bkEnsemble = null;
        }
    }

    /***
     * Inject a delay for the following requests to the specified bookie in the ensemble of the current ledger of
     * the topic.
     * @param bkIndexOfEnsemble the bk index of the ensemble.
     * @return a cancellation of the injection.
     */
    protected Closeable injectBKServerDelayForCurrentLedger(String topic, long delayTime, TimeUnit unit,
                                                            int bkIndexOfEnsemble) throws Exception {
        // Make an injection to let the next publishing delay.
        ManagedLedgerFactoryImpl mlFactory = (ManagedLedgerFactoryImpl) pulsar.getDefaultManagedLedgerFactory();
        BookieClientImpl bookieClient =
                (BookieClientImpl) mlFactory.getBookKeeper().get().getClientCtx().getBookieClient();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        LedgerHandle ledgerHandle = ml.getCurrentLedger();
        BookieId bookieId1 = ledgerHandle.getLedgerMetadata().getEnsembleAt(bkIndexOfEnsemble).get(0);
        PerChannelBookieClientPool perChannelBookieClientPool = bookieClient.lookupClient(bookieId1);
        CompletableFuture<Channel> channelFuture = new CompletableFuture<>();
        perChannelBookieClientPool.obtain(new BookkeeperInternalCallbacks.GenericCallback<PerChannelBookieClient>() {
            @Override
            public void operationComplete(int rc, PerChannelBookieClient result) {
                channelFuture.complete(WhiteboxImpl.getInternalState(result, "channel"));
            }
        }, ledgerHandle.getId());
        Channel channel = channelFuture.get();

        ScheduledExecutorService bkServerIoMock =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("bk-server-io-mock"));
        channel.eventLoop().execute(() -> {
            // To avoid client IO thread being stuck, we use a separate event loop to mock the server delay. And the
            // response handling is still in the client IO thread.
            channel.pipeline().addAfter("bookieProtoDecoder", "delayInjection", new ChannelInboundHandlerAdapter(){
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    bkServerIoMock.schedule(() -> {
                        // Call the real client io thread to handle the requests after a delayed time.
                        ctx.executor().execute(() -> {
                            ctx.fireChannelRead(msg);
                        });
                    }, delayTime, unit);
                }
            });
        });
        return () -> {
            CompletableFuture<Void> removing = new CompletableFuture<>();
            channel.eventLoop().execute(() -> {
                channel.pipeline().remove("delayInjection");
                removing.complete(null);
            });
            removing.whenComplete((__, ex) -> {
                bkServerIoMock.shutdown();
            });
        };
    }

}

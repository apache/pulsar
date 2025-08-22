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
package org.apache.pulsar.broker.cache;

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.pulsar.broker.MultiBrokerTestZKBaseTest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public abstract class AbstractBrokerEntryCacheMultiBrokerTest extends MultiBrokerTestZKBaseTest {
    public enum BrokerEntryCacheType {
        PIP430("PIP430", AbstractBrokerEntryCacheMultiBrokerTest::configurePIP430),
        PIP430Disabled("PIP430disabled", AbstractBrokerEntryCacheMultiBrokerTest::disablePIP430),
        CacheEvictionByMarkDeletedPosition("cacheEvictionByMarkDeletedPosition",
                AbstractBrokerEntryCacheMultiBrokerTest::configureCacheEvictionByMarkDeletedPosition),
        PR12258Caching("PR12258", AbstractBrokerEntryCacheMultiBrokerTest::configurePR12258Caching);

        private final String description;
        private final java.util.function.Consumer<ServiceConfiguration> configurer;

        BrokerEntryCacheType(String description, java.util.function.Consumer<ServiceConfiguration> configurer) {
            this.description = description;
            this.configurer = configurer;
        }

        public void configure(ServiceConfiguration config) {
            configurer.accept(config);
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * Configures PIP430 caching explicitly, although it's enabled by default.
     * @param conf ServiceConfiguration instance to be configured
     */
    private static void configurePIP430(ServiceConfiguration conf) {
        // use cache eviction by expected read count, this is the default behavior so it's not necessary to set it,
        // but it makes the test more explicit
        conf.setCacheEvictionByExpectedReadCount(true);
        // LRU cache eviction behavior is enabled by default, but we set it explicitly
        conf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(true);
    }

    /**
     * Disables PIP430 caching features.
     * @param conf ServiceConfiguration instance to be configured
     */
    private static void disablePIP430(ServiceConfiguration conf) {
        conf.setCacheEvictionByExpectedReadCount(false);
        conf.setManagedLedgerCacheEvictionExtendTTLOfRecentlyAccessed(false);
    }

    /**
     * Configures ServiceConfiguration for cache eviction based on the slowest markDeletedPosition.
     * This method disables cache eviction by expected read count and enables eviction by markDeletedPosition.
     * Related to https://github.com/apache/pulsar/pull/14985 - "Evicting cache data by the slowest markDeletedPosition"
     *
     * @param conf ServiceConfiguration instance to be configured
     */
    private static void configureCacheEvictionByMarkDeletedPosition(ServiceConfiguration conf) {
        disablePIP430(conf);
        conf.setCacheEvictionByMarkDeletedPosition(true);
    }

    /**
     * Configures ServiceConfiguration with settings to test PR12258 behavior for caching to drain backlog consumers.
     * This method sets configurations to enable caching for cursors with backlogged messages.
     * To make PR12258 effective, there's an additional change made in the broker codebase to
     * activate the cursor when a consumer connects, instead of waiting for the scheduled task to activate it.
     * Check org.apache.pulsar.broker.service.persistent.PersistentSubscription#addConsumerInternal method
     * for the change.
     * @param conf ServiceConfiguration instance to be modified
     */
    private static void configurePR12258Caching(ServiceConfiguration conf) {
        disablePIP430(conf);
        conf.setManagedLedgerMinimumBacklogCursorsForCaching(1);
        conf.setManagedLedgerMinimumBacklogEntriesForCaching(1);
        conf.setManagedLedgerMaxBacklogBetweenCursorsForCaching(Integer.MAX_VALUE);
        conf.setManagedLedgerCursorBackloggedThreshold(Long.MAX_VALUE);
    }

    AtomicInteger bkReadCount = new AtomicInteger(0);
    AtomicInteger bkReadEntryCount = new AtomicInteger(0);
    protected static final DateTimeFormatter CSV_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    protected final BrokerEntryCacheType cacheType;

    public AbstractBrokerEntryCacheMultiBrokerTest(BrokerEntryCacheType cacheType) {
        this.cacheType = cacheType;
    }

    @Override
    protected void beforeSetup() {
        if (!BooleanUtils.toBoolean(System.getenv("ENABLE_MANUAL_TEST")) && !isRunningInIntelliJ()) {
            throw new SkipException("This test requires setting ENABLE_MANUAL_TEST=true environment variable.");
        }
    }

    static boolean isRunningInIntelliJ() {
        // Check for IntelliJ-specific system properties
        return System.getProperty("idea.test.cyclic.buffer.size") != null
                || System.getProperty("java.class.path", "").contains("idea_rt.jar");
    }

    @Override
    protected void additionalSetup() throws Exception {
        super.additionalSetup();
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns");
        beforeMethod();
        PulsarMockBookKeeper mockBookKeeper = pulsarTestContext.getMockBookKeeper();
        mockBookKeeper.setReadHandleInterceptor(
                (long ledgerId, long firstEntry, long lastEntry, LedgerEntries entries) -> {
                    bkReadCount.incrementAndGet();
                    int numberOfEntries = (int) (lastEntry - firstEntry + 1);
                    bkReadEntryCount.addAndGet(numberOfEntries);
                    logBKRead(ledgerId, firstEntry, lastEntry, numberOfEntries);
                    return mockBookKeeper.getJfrReadHandleInterceptor()
                            .interceptReadAsync(ledgerId, firstEntry, lastEntry, entries);
                });
        // disable delays in bookkeeper writes and reads since they can skew the test results
        mockBookKeeper.setDefaultAddEntryDelayMillis(0L);
        mockBookKeeper.setDefaultReadEntriesDelayMillis(0L);
    }

    protected void logBKRead(long ledgerId, long firstEntry, long lastEntry, int numberOfEntries) {
        log.info("BK read for ledgerId {}, firstEntry {}, lastEntry {}, numberOfEntries {}",
                ledgerId, firstEntry, lastEntry, numberOfEntries);
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        customizeAllPulsarTestContextBuilders(pulsarTestContextBuilder);
    }

    @Override
    protected void customizeAdditionalPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeAdditionalPulsarTestContextBuilder(pulsarTestContextBuilder);
        customizeAllPulsarTestContextBuilders(pulsarTestContextBuilder);
    }

    protected void customizeAllPulsarTestContextBuilders(PulsarTestContext.Builder pulsarTestContextBuilder) {
        pulsarTestContextBuilder.spyNoneByDefault();
        pulsarTestContextBuilder.entryCacheEntryLengthFunction(this::calculateEntryLength);
    }

    protected int calculateEntryLength(ManagedLedgerImpl ml, Entry entry) {
        return entry.getLength();
    }

    @Override
    protected int numberOfAdditionalBrokers() {
        // test with 2 brokers in the cluster
        return 1;
    }

    @Override
    protected boolean useDynamicBrokerPorts() {
        return false;
    }

    @BeforeMethod(alwaysRun = true)
    public final void doBeforeMethod() {
        beforeMethod();
    }

    protected void beforeMethod() {
        bkReadCount.set(0);
        bkReadEntryCount.set(0);
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        configureDefaults(conf);
    }

    private void configureDefaults(ServiceConfiguration conf) {
        // configure ledger rollover to avoid OOM in test runs and slowdowns due to GC happening frequently when
        // available heap memory is low
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        conf.setManagedLedgerMaxLedgerRolloverTimeMinutes(1);
        conf.setManagedLedgerMaxEntriesPerLedger(100000);

        // set cache size
        // the entry sizes will be simulated using an injected EntryLengthFunction which
        // maps to the calculateEntryLength method in this class.
        conf.setManagedLedgerCacheSizeMB(getManagedLedgerCacheSizeMB());

        // Adjust dispatcher retry backoff to save CPU with minimal latency
        conf.setDispatcherRetryBackoffInitialTimeInMs(1);
        conf.setDispatcherRetryBackoffMaxTimeInMs(1);

        // configure the cache type
        cacheType.configure(conf);
    }

    protected int getManagedLedgerCacheSizeMB() {
        return 500;
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        ServiceConfiguration conf = super.createConfForAdditionalBroker(additionalBrokerIndex);
        configureDefaults(conf);
        return conf;
    }

    String serviceUrlForFixedPorts() {
        return "pulsar://127.0.0.1:" + mainBrokerPort + "," + additionalBrokerPorts
                .stream()
                .map(port -> "127.0.0.1:" + port)
                .collect(Collectors.joining(",")) + "/";
    }


    protected PulsarClientImpl createPulsarClient(EventLoopGroup eventLoopGroup, LongSupplier connectionDelaySupplier)
            throws Exception {
        return InjectedClientCnxClientBuilder.builder()
                .eventLoopGroup(eventLoopGroup)
                .clientBuilder(
                        PulsarClient.builder()
                                .serviceUrl(serviceUrlForFixedPorts())
                                .statsInterval(0, TimeUnit.SECONDS)
                                .memoryLimit(32, SizeUnit.MEGA_BYTES)
                )
                .connectToAddressFutureCustomizer((Channel channel, InetSocketAddress inetSocketAddress) -> {
                    // Introduce a delay to simulate network latency in connecting to the broker.
                    long delayMillis = connectionDelaySupplier.getAsLong();
                    if (delayMillis > 0) {
                        return CompletableFuture.supplyAsync(() -> null,
                                        CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS))
                                .thenCompose(
                                        __ -> ChannelFutures.toCompletableFuture(channel.connect(inetSocketAddress)));
                    } else {
                        return ChannelFutures.toCompletableFuture(channel.connect(inetSocketAddress));
                    }
                })
                .build();
    }

    protected static String formatTimestampForCsv(long ts) {
        return CSV_DATETIME_FORMATTER.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC));
    }
}

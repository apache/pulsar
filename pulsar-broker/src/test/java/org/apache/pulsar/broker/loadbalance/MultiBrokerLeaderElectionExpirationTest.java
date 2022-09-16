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

package org.apache.pulsar.broker.loadbalance;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerTestZKBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MultiBrokerLeaderElectionExpirationTest extends MultiBrokerTestZKBaseTest {
    private static final long EXPIRE_AFTER_WRITE_MILLIS_IN_TEST = 2000L;
    private static final long REFRESH_AFTER_WRITE_MILLIS_IN_TEST = 1000L;

    @Override
    protected int numberOfAdditionalBrokers() {
        return 9;
    }

    @Test
    public void shouldElectOneLeader() {
        int leaders = 0;
        for (PulsarService broker : getAllBrokers()) {
            if (broker.getLeaderElectionService().isLeader()) {
                leaders++;
            }
        }
        assertEquals(leaders, 1);
    }

    @Override
    protected MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
        return changeDefaultMetadataCacheConfig(super.createLocalMetadataStore());
    }

    @Override
    protected MetadataStoreExtended createConfigurationMetadataStore() throws MetadataStoreException {
        return changeDefaultMetadataCacheConfig(super.createConfigurationMetadataStore());
    }

    MetadataStoreExtended changeDefaultMetadataCacheConfig(MetadataStoreExtended metadataStore) {
        MetadataStoreExtended spy = spy(metadataStore);
        when(spy.getDefaultMetadataCacheConfig()).thenReturn(MetadataCacheConfig
                .builder()
                .refreshAfterWriteMillis(REFRESH_AFTER_WRITE_MILLIS_IN_TEST)
                .expireAfterWriteMillis(EXPIRE_AFTER_WRITE_MILLIS_IN_TEST)
                .build());
        return spy;
    }

    @Test
    public void shouldAllBrokersBeAbleToGetTheLeaderAfterExpiration()
            throws ExecutionException, InterruptedException, TimeoutException {

        // if you want to see this test fail, modify the line in LeaderElectionImpl constructor for creating
        // the metadata cache to not skip expirations:
        // this.cache = store.getMetadataCache(clazz);

        // Given that all brokers have the leader elected
        Awaitility.await().untilAsserted(() -> {
            for (PulsarService broker : getAllBrokers()) {
                Optional<LeaderBroker> currentLeader = broker.getLeaderElectionService().getCurrentLeader();
                assertTrue(currentLeader.isPresent(), "Leader wasn't known on broker " + broker.getBrokerServiceUrl());
            }
        });

        // Wait for metadata cache entries to expire
        Thread.sleep(EXPIRE_AFTER_WRITE_MILLIS_IN_TEST);

        // then leader should be known on all brokers and it should be the same leader
        LeaderBroker leader = null;
        for (PulsarService broker : getAllBrokers()) {
            Optional<LeaderBroker> currentLeader =
                    broker.getLeaderElectionService().readCurrentLeader().get(1, TimeUnit.SECONDS);
            assertTrue(currentLeader.isPresent(), "Leader wasn't known on broker " + broker.getBrokerServiceUrl());
            if (leader != null) {
                assertEquals(currentLeader.get(), leader,
                        "Different leader on broker " + broker.getBrokerServiceUrl());
            } else {
                leader = currentLeader.get();
            }
        }
    }
}

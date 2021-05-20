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
package org.apache.pulsar.broker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockedBookKeeperClientFactory implements BookKeeperClientFactory {
    private static final Logger log = LoggerFactory.getLogger(MockedBookKeeperClientFactory.class);

    private final BookKeeper mockedBk;
    private final OrderedExecutor executor;

    public MockedBookKeeperClientFactory() {
        try {
            executor = OrderedExecutor.newBuilder()
                    .numThreads(1)
                    .name("mock-bk-client-factory")
                    .build();
            mockedBk = new PulsarMockBookKeeper(executor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, EventLoopGroup eventLoopGroup,
            Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
            Map<String, Object> properties) throws IOException {
        return mockedBk;
    }

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties, StatsLogger statsLogger) throws IOException {
        return mockedBk;
    }

    @Override
    public void close() {
        try {
            mockedBk.close();
        } catch (BKException | InterruptedException ignored) {
        }
        executor.shutdownNow();
    }
}

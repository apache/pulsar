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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.apache.pulsar.zookeeper.ZooKeeperSessionWatcher.ShutdownService;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagingServiceShutdownHook extends Thread implements ShutdownService {

    private static final Logger LOG = LoggerFactory.getLogger(MessagingServiceShutdownHook.class);

    private PulsarService service = null;
    private final Consumer<Integer> processTerminator;

    public MessagingServiceShutdownHook(PulsarService service, Consumer<Integer> processTerminator) {
        this.service = service;
        this.processTerminator = processTerminator;
    }

    @Override
    public void run() {
        if (service.getConfiguration() != null) {
            LOG.info("messaging service shutdown hook started, lookup webservice="
                    + service.getSafeWebServiceAddress() + ", broker url=" + service.getSafeBrokerServiceUrl());
        }

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("shutdown-thread"));

        try {
            CompletableFuture<Void> future = new CompletableFuture<>();

            executor.execute(() -> {
                try {
                    service.closeAsync().whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            future.completeExceptionally(throwable);
                        } else {
                            future.complete(result);
                        }
                    });
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            });

            future.get(service.getConfiguration().getBrokerShutdownTimeoutMs(), TimeUnit.MILLISECONDS);

            LOG.info("Completed graceful shutdown. Exiting");
        } catch (TimeoutException | CancellationException e) {
            LOG.warn("Graceful shutdown timeout expired. Closing now");
        } catch (Exception e) {
            LOG.error("Failed to perform graceful shutdown, Exiting anyway", e);
        } finally {
            // always put system to halt immediately
            processTerminator.accept(0);
        }
    }

    @Override
    public void shutdown(int exitCode) {
        try {
            // Try to close ZK session to ensure all ephemeral locks gets released immediately
            if (service != null) {
                if (service.getZkClient().getState() != States.CLOSED) {
                    service.getZkClient().close();
                }
            }
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }

        LOG.info("Invoking Runtime.halt({})", exitCode);
        processTerminator.accept(exitCode);
    }
}

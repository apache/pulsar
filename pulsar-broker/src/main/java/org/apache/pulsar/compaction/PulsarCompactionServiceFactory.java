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
package org.apache.pulsar.compaction;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;

public class PulsarCompactionServiceFactory implements CompactionServiceFactory {

    @Getter(AccessLevel.PROTECTED)
    private PulsarService pulsarService;

    private volatile Compactor compactor;

    @VisibleForTesting
    public Compactor getCompactor() throws PulsarServerException {
        if (compactor == null) {
            synchronized (this) {
                if (compactor == null) {
                    compactor = newCompactor();
                }
            }
        }
        return compactor;
    }

    @Nullable
    public Compactor getNullableCompactor() {
        return compactor;
    }

    protected Compactor newCompactor() throws PulsarServerException {
        return new PublishingOrderCompactor(pulsarService.getConfiguration(),
                pulsarService.getClient(), pulsarService.getBookKeeperClient(),
                pulsarService.getCompactorExecutor());
    }

    @Override
    public CompletableFuture<Void> initialize(@Nonnull PulsarService pulsarService) {
        Objects.requireNonNull(pulsarService);
        this.pulsarService = pulsarService;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TopicCompactionService> newTopicCompactionService(@Nonnull String topic) {
        Objects.requireNonNull(topic);
        PulsarTopicCompactionService pulsarTopicCompactionService =
                new PulsarTopicCompactionService(topic, pulsarService.getBookKeeperClient(), () -> {
                    try {
                        return this.getCompactor();
                    } catch (Throwable e) {
                        throw new CompletionException(e);
                    }
                });
        return CompletableFuture.completedFuture(pulsarTopicCompactionService);
    }

    @Override
    public void close() throws Exception {
        // noop
    }
}

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
package org.apache.bookkeeper.mledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerOfflineBacklog;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.testng.annotations.Test;

public class ManagedLedgerFactoryBacklogTest {

    @Test
    public void testLegacyImplementationUsesSuppliedExecutorAndPublishesSnapshot() throws Exception {
        ManagedLedgerFactory factory = legacyFactory();
        TopicName topicName = TopicName.get("persistent://tenant/namespace/topic");
        PersistentOfflineTopicStats stats = new PersistentOfflineTopicStats("topic", "broker");
        Queue<Runnable> tasks = new ArrayDeque<>();
        AtomicReference<PersistentOfflineTopicStats> collected = new AtomicReference<>();
        Object context = new Object();
        doAnswer(invocation -> {
            PersistentOfflineTopicStats value = invocation.getArgument(0);
            collected.set(value);
            value.totalMessages = 5;
            value.storageSize = 50;
            value.messageBacklog = 3;
            value.addLedgerDetails(5, 10, 50, 1);
            value.addCursorDetails("subscription", 3, 2);
            value.statGeneratedAt.setTime(10);
            return null;
        }).when(factory).estimateUnloadedTopicBacklog(any(), eq(topicName), eq(true), eq(context));

        CompletableFuture<Void> future = factory.estimateUnloadedTopicBacklogAsync(
                stats, topicName, true, context, tasks::add);
        assertThat(future).isNotDone();
        assertThat(collected.get()).isNull();
        tasks.remove().run();
        future.get(5, TimeUnit.SECONDS);

        assertThat(stats.totalMessages).isEqualTo(5);
        assertThat(stats.storageSize).isEqualTo(50);
        assertThat(stats.messageBacklog).isEqualTo(3);
        assertThat(stats.dataLedgerDetails).hasSize(1);
        assertThat(stats.cursorDetails.get("subscription").cursorBacklog).isEqualTo(3);
        assertThat(stats.statGeneratedAt.getTime()).isEqualTo(10);

        collected.get().dataLedgerDetails.get(0).entries = 100;
        collected.get().cursorDetails.get("subscription").cursorBacklog = 100;
        collected.get().statGeneratedAt.setTime(100);
        collected.get().messageBacklog = 100;
        assertThat(stats.dataLedgerDetails.get(0).entries).isEqualTo(5);
        assertThat(stats.cursorDetails.get("subscription").cursorBacklog).isEqualTo(3);
        assertThat(stats.statGeneratedAt.getTime()).isEqualTo(10);
        assertThat(stats.messageBacklog).isEqualTo(3);
    }

    @Test
    public void testLegacyFailureDoesNotPublishPartialStats() throws Exception {
        ManagedLedgerFactory factory = legacyFactory();
        TopicName topicName = TopicName.get("persistent://tenant/namespace/topic");
        PersistentOfflineTopicStats stats = new PersistentOfflineTopicStats("topic", "broker");
        Exception failure = new Exception("metadata read failed");
        doAnswer(invocation -> {
            PersistentOfflineTopicStats value = invocation.getArgument(0);
            value.messageBacklog = 3;
            value.addCursorDetails("subscription", 3, 2);
            throw failure;
        }).when(factory).estimateUnloadedTopicBacklog(any(), eq(topicName), eq(false), isNull());

        CompletableFuture<Void> future = factory.estimateUnloadedTopicBacklogAsync(
                stats, topicName, false, null, Runnable::run);
        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS)).hasCause(failure);
        assertThat(stats.messageBacklog).isZero();
        assertThat(stats.cursorDetails).isEmpty();
    }

    @Test
    public void testLegacySubmissionAndValidationFailuresCompleteFuture() {
        ManagedLedgerFactory factory = legacyFactory();
        TopicName topicName = TopicName.get("persistent://tenant/namespace/topic");
        PersistentOfflineTopicStats stats = new PersistentOfflineTopicStats("topic", "broker");
        RejectedExecutionException rejection = new RejectedExecutionException("executor stopped");
        Executor executor = command -> {
            throw rejection;
        };
        CompletableFuture<Void> rejected = factory.estimateUnloadedTopicBacklogAsync(
                stats, topicName, false, null, executor);
        assertThatThrownBy(() -> rejected.get(5, TimeUnit.SECONDS)).hasCause(rejection);
        assertThat(factory.estimateUnloadedTopicBacklogAsync(null, topicName, false, null, executor))
                .isCompletedExceptionally();
        assertThat(factory.estimateUnloadedTopicBacklogAsync(stats, null, false, null, executor))
                .isCompletedExceptionally();
        assertThat(factory.estimateUnloadedTopicBacklogAsync(stats, topicName, false, null, null))
                .isCompletedExceptionally();
    }

    @Test
    public void testAsyncHelperPreservesLegacyContext() throws Exception {
        ManagedLedgerFactory factory = legacyFactory();
        TopicName topicName = TopicName.get("persistent://tenant/namespace/topic");
        Queue<Runnable> tasks = new ArrayDeque<>();
        ManagedLedgerOfflineBacklog backlog = new ManagedLedgerOfflineBacklog(
                DigestType.CRC32C, new byte[0], "broker", false);
        doAnswer(invocation -> {
            PersistentOfflineTopicStats stats = invocation.getArgument(0);
            stats.messageBacklog = 3;
            return null;
        }).when(factory).estimateUnloadedTopicBacklog(any(), eq(topicName), eq(false), isNull());

        CompletableFuture<PersistentOfflineTopicStats> future = backlog.estimateUnloadedTopicBacklogAsync(
                factory, topicName, tasks::add);
        assertThat(future).isNotDone();
        tasks.remove().run();
        PersistentOfflineTopicStats stats = future.get(5, TimeUnit.SECONDS);
        assertThat(stats.messageBacklog).isEqualTo(3);
        assertThat(stats.topicName).isEqualTo(topicName.getPersistenceNamingEncoding());
        assertThat(stats.brokerName).isEqualTo("broker");
    }

    @Test
    public void testAsyncHelperReportsInvalidArgumentsThroughFuture() {
        ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
        ManagedLedgerOfflineBacklog backlog = new ManagedLedgerOfflineBacklog(
                DigestType.CRC32C, new byte[0], "broker", false);
        assertThat(backlog.estimateUnloadedTopicBacklogAsync(factory, null, Runnable::run))
                .isCompletedExceptionally();
        assertThat(backlog.estimateUnloadedTopicBacklogAsync(null,
                TopicName.get("persistent://tenant/namespace/topic"), Runnable::run)).isCompletedExceptionally();
        verifyNoInteractions(factory);
    }

    private static ManagedLedgerFactory legacyFactory() {
        ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
        doCallRealMethod().when(factory).estimateUnloadedTopicBacklogAsync(any(), any(), anyBoolean(),
                any(), any());
        return factory;
    }
}

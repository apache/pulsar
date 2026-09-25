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
package org.apache.pulsar.metadata.impl.oxia;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.options.PutOption;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

class OxiaSessionWatcherTest {

    private AsyncOxiaClient client;
    private List<SessionEvent> events;
    private AtomicInteger putCalls;
    private AtomicInteger getCalls;
    private AtomicReference<String> putKey;
    private AtomicReference<Set<PutOption>> putOptions;

    @BeforeMethod
    void setup() {
        client = mock(AsyncOxiaClient.class);
        events = new CopyOnWriteArrayList<>();
        putCalls = new AtomicInteger();
        getCalls = new AtomicInteger();
        putKey = new AtomicReference<>();
        putOptions = new AtomicReference<>();
        stubPut(() -> CompletableFuture.completedFuture(null));
    }

    private Answer<CompletableFuture<PutResult>> recording(Supplier<CompletableFuture<PutResult>> outcome) {
        return invocation -> {
            putKey.set(invocation.getArgument(0));
            putOptions.set(invocation.getArgument(2));
            putCalls.incrementAndGet();
            return outcome.get();
        };
    }

    private void stubPut(Supplier<CompletableFuture<PutResult>> outcome) {
        doAnswer(recording(outcome)).when(client).put(anyString(), any(), any());
    }

    private OxiaSessionWatcher startedWatcher() {
        OxiaSessionWatcher watcher = new OxiaSessionWatcher(client, events::add);
        watcher.start();
        await().atMost(5, SECONDS).until(() -> putKey.get() != null);
        awaitSessionEstablished(watcher);
        return watcher;
    }

    /**
     * Deterministically waits for the ESTABLISHED state: the existence check only issues its
     * read once the canary write completed, so a read that was issued proves the state. The
     * probe read is answered with a present canary, which is a no-op.
     */
    private void awaitSessionEstablished(OxiaSessionWatcher watcher) {
        CompletableFuture<GetResult> probe = new CompletableFuture<>();
        doAnswer(invocation -> {
            getCalls.incrementAndGet();
            return probe;
        }).when(client).get(anyString());
        await().atMost(5, SECONDS).until(() -> {
            watcher.checkCanary();
            return getCalls.get() >= 1;
        });
        probe.complete(mock(GetResult.class));
    }

    @Test
    void deletionOfOwnCanaryFiresLostThenReestablishedOnRecreation() {
        try (OxiaSessionWatcher watcher = startedWatcher()) {
            watcher.handleNotification(new Notification.KeyDeleted(putKey.get()));

            await().atMost(5, SECONDS)
                    .untilAsserted(() -> assertThat(events).containsExactly(
                            SessionEvent.SessionLost, SessionEvent.SessionReestablished));
            assertThat(putCalls.get()).isEqualTo(2);
            assertThat(putKey.get()).startsWith(OxiaSessionWatcher.CANARY_KEY_PREFIX);
            assertThat(putOptions.get()).containsExactly(PutOption.AsEphemeralRecord);
        }
    }

    @Test
    void foreignCanaryDeletionsAreIgnored() {
        try (OxiaSessionWatcher watcher = startedWatcher()) {
            watcher.handleNotification(
                    new Notification.KeyDeleted(OxiaSessionWatcher.CANARY_KEY_PREFIX + "another-instance"));

            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(events).isEmpty();
                        assertThat(putCalls.get()).isEqualTo(1);
                    });
        }
    }

    @Test
    void creationAndModificationOfOwnCanaryAreIgnored() {
        try (OxiaSessionWatcher watcher = startedWatcher()) {
            watcher.handleNotification(new Notification.KeyCreated(putKey.get(), 1L));
            watcher.handleNotification(new Notification.KeyModified(putKey.get(), 2L));

            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(events).isEmpty();
                        assertThat(putCalls.get()).isEqualTo(1);
                    });
        }
    }

    @Test
    void existenceCheckDetectsAMissingCanary() {
        try (OxiaSessionWatcher watcher = startedWatcher()) {
            doReturn(CompletableFuture.completedFuture(null)).when(client).get(anyString());
            watcher.checkCanary();

            await().atMost(5, SECONDS)
                    .untilAsserted(() -> assertThat(events).containsExactly(
                            SessionEvent.SessionLost, SessionEvent.SessionReestablished));
        }
    }

    @Test
    void existenceCheckIgnoresAPresentCanaryAndAFailingRead() {
        try (OxiaSessionWatcher watcher = startedWatcher()) {
            doReturn(CompletableFuture.completedFuture(mock(GetResult.class))).when(client).get(anyString());
            watcher.checkCanary();
            doReturn(CompletableFuture.failedFuture(new RuntimeException("down"))).when(client).get(anyString());
            watcher.checkCanary();

            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> assertThat(events).isEmpty());
        }
    }

    @Test
    void existenceCheckDoesNothingBeforeTheSessionIsEstablished() {
        stubPut(() -> new CompletableFuture<>());

        try (OxiaSessionWatcher watcher = new OxiaSessionWatcher(client, events::add)) {
            watcher.start();
            await().atMost(5, SECONDS).until(() -> putKey.get() != null);

            watcher.checkCanary();

            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> assertThat(events).isEmpty());
        }
    }

    @Test
    void existenceCheckRacingARecreatingWriteIsDiscarded() {
        try (OxiaSessionWatcher watcher = startedWatcher()) {
            // The existence check is issued while the canary is still present, but its read
            // only completes after the canary was deleted and recreated in the meantime.
            CompletableFuture<GetResult> checkRead = new CompletableFuture<>();
            doReturn(checkRead).when(client).get(anyString());
            watcher.checkCanary();

            // The canary is deleted while the read is in flight: the loss is reported and the
            // recreation write is issued and stays in flight.
            CompletableFuture<PutResult> recreation = new CompletableFuture<>();
            stubPut(() -> recreation);
            watcher.handleNotification(new Notification.KeyDeleted(putKey.get()));
            await().atMost(5, SECONDS).until(() -> events.contains(SessionEvent.SessionLost));
            await().atMost(5, SECONDS).until(() -> putCalls.get() == 2);

            // The recreation write lands first, then the stale read, which saw the deleted
            // canary: the read must be discarded, instead of reporting a second loss for an
            // already live session.
            recreation.complete(null);
            await().atMost(5, SECONDS).until(() -> events.contains(SessionEvent.SessionReestablished));
            checkRead.complete(null);

            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(events).containsExactly(
                                SessionEvent.SessionLost, SessionEvent.SessionReestablished);
                        assertThat(putCalls.get()).isEqualTo(2);
                    });
        }
    }

    @Test
    void failedCanaryWriteIsRetriedUntilItSucceeds() {
        doAnswer(recording(() -> CompletableFuture.failedFuture(new RuntimeException("down"))))
                .doAnswer(recording(() -> CompletableFuture.completedFuture(null)))
                .when(client).put(anyString(), any(), any());

        try (OxiaSessionWatcher watcher = new OxiaSessionWatcher(client, events::add)) {
            watcher.start();

            // The first write fails and is retried after the backoff delay; the retry
            // establishes the canary without ever reporting a session event, because nothing
            // was lost yet.
            await().atMost(5, SECONDS).until(() -> putCalls.get() == 2);
            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> assertThat(events).isEmpty());
        }
    }

    @Test
    void closedWatcherIgnoresNotifications() {
        OxiaSessionWatcher watcher = startedWatcher();
        watcher.close();

        watcher.handleNotification(new Notification.KeyDeleted(putKey.get()));

        await().during(1, SECONDS).atMost(3, SECONDS)
                .untilAsserted(() -> assertThat(events).isEmpty());
    }
}

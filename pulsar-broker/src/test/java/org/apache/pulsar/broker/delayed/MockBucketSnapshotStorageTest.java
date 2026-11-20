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
package org.apache.pulsar.broker.delayed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MockBucketSnapshotStorageTest {

    @Test
    public void testConcurrentDeleteAndClean() throws Exception {
        MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage();
        List<Long> ids = new ArrayList<>();
        List<CompletableFuture<Void>> deletes = new ArrayList<>();
        try {
            for (int i = 0; i < 128; i++) {
                ids.add(storage.createBucketSnapshot(metadata(), List.of(), "key", "topic", "cursor")
                        .get(10, TimeUnit.SECONDS));
            }
            for (long id : ids) {
                deletes.add(storage.deleteBucketSnapshot(id));
            }
            storage.close();
            CompletableFuture.allOf(deletes.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        } finally {
            try {
                for (CompletableFuture<Void> delete : deletes) {
                    delete.handle((value, error) -> null).get(10, TimeUnit.SECONDS);
                }
            } finally {
                storage.close();
            }
        }
    }

    @Test
    public void testCloseWaitsForPendingRead() throws Exception {
        GatedExecutor executor = new GatedExecutor();
        MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage(executor);
        try (executor;
                ExecutorService closingExecutor = Executors.newSingleThreadExecutor(
                        new DefaultThreadFactory("bucket-snapshot-storage-close-test"))) {
            try {
                SnapshotMetadata metadata = new SnapshotMetadata();
                metadata.addMetadata().setMinScheduleTimestamp(1).setMaxScheduleTimestamp(1);
                long bucketId = storage.createBucketSnapshot(metadata, List.of(), "key", "topic", "cursor")
                        .get(10, TimeUnit.SECONDS);

                executor.gateNextTask.set(true);
                CompletableFuture<SnapshotMetadata> reading = storage.getBucketSnapshotMetadata(bucketId);
                executor.taskStarted.get(10, TimeUnit.SECONDS);
                CompletableFuture<Void> closing = CompletableFuture.runAsync(storage::clean, closingExecutor);
                executor.shutdownStarted.get(10, TimeUnit.SECONDS);

                assertThat(closing).as("close must wait for the pending storage read").isNotDone();
                executor.releaseTask.complete(null);
                SnapshotMetadata recovered = reading.get(10, TimeUnit.SECONDS);
                assertThat(recovered.getMetadataAt(0).getMinScheduleTimestamp()).isEqualTo(1);
                closing.get(10, TimeUnit.SECONDS);
            } finally {
                executor.releaseTask.complete(null);
            }
        } finally {
            storage.close();
        }
    }

    @Test
    public void testInterruptedClosePreservesPendingRead() throws Exception {
        try (GatedExecutor executor = new GatedExecutor()) {
            MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage(executor);
            try {
                SnapshotMetadata metadata = new SnapshotMetadata();
                metadata.addMetadata().setMinScheduleTimestamp(1).setMaxScheduleTimestamp(1);
                long bucketId = storage.createBucketSnapshot(metadata, List.of(), "key", "topic", "cursor")
                        .get(10, TimeUnit.SECONDS);

                executor.gateNextTask.set(true);
                CompletableFuture<SnapshotMetadata> reading = storage.getBucketSnapshotMetadata(bucketId);
                executor.taskStarted.get(10, TimeUnit.SECONDS);

                Thread.currentThread().interrupt();
                try {
                    assertThatThrownBy(storage::close).isInstanceOf(IllegalStateException.class);
                    assertThat(Thread.currentThread().isInterrupted()).as("preserve the caller's interrupt").isTrue();
                } finally {
                    Thread.interrupted();
                }

                executor.releaseTask.complete(null);
                SnapshotMetadata recovered = reading.get(10, TimeUnit.SECONDS);
                assertThat(recovered.getMetadataAt(0).getMinScheduleTimestamp()).isEqualTo(1);
            } finally {
                Thread.interrupted();
                executor.releaseTask.complete(null);
                storage.close();
            }
        }
    }

    @Test
    public void testCloseTimeoutPreservesPendingRead() throws Exception {
        try (GatedExecutor executor = new GatedExecutor()) {
            MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage(executor);
            try {
                SnapshotMetadata metadata = new SnapshotMetadata();
                metadata.addMetadata().setMinScheduleTimestamp(1).setMaxScheduleTimestamp(1);
                long bucketId = storage.createBucketSnapshot(metadata, List.of(), "key", "topic", "cursor")
                        .get(10, TimeUnit.SECONDS);

                executor.gateNextTask.set(true);
                CompletableFuture<SnapshotMetadata> reading = storage.getBucketSnapshotMetadata(bucketId);
                executor.taskStarted.get(10, TimeUnit.SECONDS);

                executor.terminationTimedOut = true;
                assertThatThrownBy(storage::close).isInstanceOf(IllegalStateException.class);

                executor.terminationTimedOut = false;
                executor.releaseTask.complete(null);
                SnapshotMetadata recovered = reading.get(10, TimeUnit.SECONDS);
                assertThat(recovered.getMetadataAt(0).getMinScheduleTimestamp()).isEqualTo(1);
            } finally {
                executor.terminationTimedOut = false;
                executor.releaseTask.complete(null);
                storage.close();
            }
        }
    }

    private static SnapshotMetadata metadata() {
        SnapshotMetadata metadata = new SnapshotMetadata();
        metadata.addMetadata().setMinScheduleTimestamp(1).setMaxScheduleTimestamp(1);
        return metadata;
    }

    private static class GatedExecutor extends ThreadPoolExecutor {
        final AtomicBoolean gateNextTask = new AtomicBoolean();
        final CompletableFuture<Void> taskStarted = new CompletableFuture<>();
        final CompletableFuture<Void> releaseTask = new CompletableFuture<>();
        final CompletableFuture<Void> shutdownStarted = new CompletableFuture<>();
        boolean terminationTimedOut;

        GatedExecutor() {
            super(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                    new DefaultThreadFactory("bucket-snapshot-storage-test"));
        }

        @Override
        protected void beforeExecute(Thread thread, Runnable task) {
            super.beforeExecute(thread, task);
            if (gateNextTask.compareAndSet(true, false)) {
                taskStarted.complete(null);
                // Hold the storage operation before it accesses its buffers.
                releaseTask.orTimeout(10, TimeUnit.SECONDS).join();
            }
        }

        @Override
        public void shutdown() {
            super.shutdown();
            shutdownStarted.complete(null);
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            if (terminationTimedOut) {
                // Simulate the timeout without waiting while the real worker remains gated.
                return false;
            }
            return super.awaitTermination(timeout, unit);
        }
    }
}

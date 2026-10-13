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
package org.apache.pulsar.tests.performance.launcher;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.tests.performance.report.RunReport;

/**
 * Seeks a subscription of its own to random publish times between the measurement's start and end, as
 * {@link SeekSettings} describes, and records each seek's time in {@link RunReport#SEEKS_FILE}. It stops at the
 * settings' timeout, or when the run fails, such as a network impairment that failed, and removes the subscriptions
 * that it created in any case.
 */
final class RandomSeeks {
    /** The launcher's subscription for the seeks. */
    static final String SUBSCRIPTION = "performance-random-seeks";
    private static final int PROGRESS_INTERVAL = 50;
    // The longest that one admin request may take
    private static final int REQUEST_TIMEOUT_SECONDS = 120;

    /** The admin operations of the seeks, which a test fakes. */
    interface TopicOperations {
        void createSubscription(String topic) throws Exception;

        void unload(String topic) throws Exception;

        void resetCursor(String topic, long publishTime) throws Exception;

        /** The position of the message at the subscription's position, or empty when none. */
        String peek(String topic) throws Exception;

        void deleteSubscription(String topic) throws Exception;
    }

    private RandomSeeks() {
    }

    static void run(String adminUrl, List<String> topics, long fromEpochMs, long toEpochMs, SeekSettings settings,
                    Path runOutput, CompletableFuture<?> runFailure, Consumer<String> status) throws Exception {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl)
                .connectionTimeout(10, TimeUnit.SECONDS)
                .readTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .requestTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build()) {
            run(adminTopicOperations(admin), topics, fromEpochMs, toEpochMs, settings, runOutput, runFailure, status);
        }
    }

    static void run(TopicOperations topicOperations, List<String> topics, long fromEpochMs, long toEpochMs,
                    SeekSettings settings, Path runOutput, CompletableFuture<?> runFailure, Consumer<String> status)
            throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(settings.timeoutSeconds());
        List<String> created = new ArrayList<>();
        Exception failure = null;
        try (BufferedWriter csv = Files.newBufferedWriter(runOutput.resolve(RunReport.SEEKS_FILE))) {
            csv.write(RunReport.SEEKS_HEADER);
            csv.newLine();
            for (String topic : topics) {
                checkRunning(runFailure, deadlineNanos, settings, 0);
                topicOperations.createSubscription(topic);
                created.add(topic);
            }
            if (settings.unloadFirst()) {
                for (String topic : topics) {
                    checkRunning(runFailure, deadlineNanos, settings, 0);
                    topicOperations.unload(topic);
                }
            }
            status.accept(String.format(Locale.ROOT, "Seeking %,d times to random publish times of %,d topic(s)%s",
                    settings.count(), topics.size(), settings.peek() ? ", reading the message at each" : ""));
            Random random = new Random(settings.seed());
            long span = Math.max(1, toEpochMs - fromEpochMs);
            long started = System.nanoTime();
            for (int seek = 0; seek < settings.count(); seek++) {
                checkRunning(runFailure, deadlineNanos, settings, seek);
                String topic = topics.get(random.nextInt(topics.size()));
                long publishTime = fromEpochMs + (long) (random.nextDouble() * span);
                long start = System.currentTimeMillis();
                long resetStart = System.nanoTime();
                topicOperations.resetCursor(topic, publishTime);
                long resetMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - resetStart);
                long peekMicros = -1;
                String position = "";
                if (settings.peek()) {
                    long peekStart = System.nanoTime();
                    position = topicOperations.peek(topic);
                    peekMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - peekStart);
                }
                csv.write(seek + "," + start + "," + System.currentTimeMillis() + "," + topic + "," + publishTime + ","
                        + resetMicros + "," + peekMicros + "," + position);
                csv.newLine();
                if ((seek + 1) % PROGRESS_INTERVAL == 0) {
                    csv.flush();
                    status.accept(String.format(Locale.ROOT, "Seeks: %,d of %,d in %.0f s", seek + 1,
                            settings.count(), (System.nanoTime() - started) / 1e9));
                }
            }
            // The last seek counts too; a request that is in progress waits at most for the admin's request timeout
            checkRunning(runFailure, deadlineNanos, settings, settings.count());
        } catch (Exception e) {
            failure = e;
        }
        // Also after a failure, so that no subscription keeps a backlog, such as in containers left running
        for (String topic : created) {
            try {
                topicOperations.deleteSubscription(topic);
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void checkRunning(CompletableFuture<?> runFailure, long deadlineNanos, SeekSettings settings,
                                     int seeks) throws Exception {
        if (runFailure.isCompletedExceptionally()) {
            try {
                runFailure.get();
            } catch (ExecutionException e) {
                throw new IllegalStateException("The run failed during the seeks", e.getCause());
            }
        }
        if (deadlineNanos - System.nanoTime() <= 0) {
            throw new TimeoutException(String.format(Locale.ROOT, "The seeks didn't finish within"
                    + " offload.seeks.timeoutSeconds, %,d s: %,d of %,d made", settings.timeoutSeconds(), seeks,
                    settings.count()));
        }
    }

    private static TopicOperations adminTopicOperations(PulsarAdmin admin) {
        return new TopicOperations() {
            @Override
            public void createSubscription(String topic) throws PulsarAdminException {
                admin.topics().createSubscription(topic, SUBSCRIPTION, MessageId.earliest);
            }

            @Override
            public void unload(String topic) throws PulsarAdminException {
                admin.topics().unload(topic);
            }

            @Override
            public void resetCursor(String topic, long publishTime) throws PulsarAdminException {
                admin.topics().resetCursor(topic, SUBSCRIPTION, publishTime);
            }

            @Override
            public String peek(String topic) throws PulsarAdminException, PulsarClientException {
                List<Message<byte[]>> messages = admin.topics().peekMessages(topic, SUBSCRIPTION, 1);
                return messages.isEmpty() ? "" : messages.get(0).getMessageId().toString();
            }

            @Override
            public void deleteSubscription(String topic) throws PulsarAdminException {
                admin.topics().deleteSubscription(topic, SUBSCRIPTION);
            }
        };
    }
}

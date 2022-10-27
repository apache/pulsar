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
package org.apache.pulsar.broker.systopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.SystemTopicBasedTopicPoliciesService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.reflect.WhiteboxImpl;

@Slf4j
public class ClientMarkerNamespaceEventsSystemTopicFactory extends NamespaceEventsSystemTopicFactory {

    private final PulsarClient client;

    public ClientMarkerNamespaceEventsSystemTopicFactory(PulsarClient client) {
        super(client);
        this.client = client;
    }

    public static void waitAllClientCreateTaskDone(Collection<PulsarService> pulsars,
                                                   List<CompletableFuture<Void>> runningEventListeners) {
        // Wait listener finish.
        waitAllFutureDoneExIgnore(runningEventListeners);

        // Wait reader/writer/topic tasks finish.
        List<CompletableFuture<Void>> loggingFutures = new ArrayList<>();
        loggingFutures.addAll(
                wrapReaderFutureWithLogging(MarkerTopicPoliciesSystemTopicClient.removeAllReaderCreateTasks()));
        loggingFutures.addAll(
                wrapWriterFutureWithLogging(MarkerTopicPoliciesSystemTopicClient.removeAllWriterCreateTasks()));

        // Topics and readers in cache.
        List<CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>> cachedReaders = new ArrayList<>();
        List<CompletableFuture<Optional<Topic>>> topics = new ArrayList<>();
        for (PulsarService pulsar : pulsars) {
            topics.addAll(pulsar.getBrokerService().getTopics().values());
            TopicPoliciesService topicPoliciesService = pulsar.getTopicPoliciesService();
            if (!(topicPoliciesService instanceof SystemTopicBasedTopicPoliciesService)) {
                continue;
            }
            SystemTopicBasedTopicPoliciesService systemTopicBasedTopicPoliciesService =
                    (SystemTopicBasedTopicPoliciesService) topicPoliciesService;
            Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>> readerCaches =
                    WhiteboxImpl.getInternalState(systemTopicBasedTopicPoliciesService, "readerCaches");
            cachedReaders.addAll(readerCaches.values());
        }
        loggingFutures.addAll(wrapTopicFutureWithLogging(topics));
        loggingFutures.addAll(wrapReaderFutureWithLogging(cachedReaders));

        // Wait ignore ex.
        waitAllFutureDoneExIgnore(loggingFutures);
    }

    @Override
    public TopicPoliciesSystemTopicClient createTopicPoliciesSystemTopicClient(NamespaceName namespaceName) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(), namespaceName,
                SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
        return new MarkerTopicPoliciesSystemTopicClient(client, topicName);
    }

    private static void waitAllFutureDoneExIgnore(List<CompletableFuture<Void>> futures) {
        try {
            FutureUtil.waitForAll(futures).get(3, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            for (CompletableFuture<Void> completableFuture : futures){
                if (completableFuture.isDone()){
                    continue;
                }
                completableFuture.completeExceptionally(timeoutException);
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
    }

    private static List<CompletableFuture<Void>> wrapReaderFutureWithLogging(
            List<CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>> readerFutures) {
        List<CompletableFuture<Void>> loggingFutures = new ArrayList<>();
        for (CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> future : readerFutures) {
            loggingFutures.add(future.thenApply(reader -> {
                log.info("===> reader in create finished. topic: {}",
                        String.valueOf(reader.getSystemTopic().getTopicName()));
                return null;
            }));
        }
        return loggingFutures;
    }

    private static List<CompletableFuture<Void>> wrapWriterFutureWithLogging(
            List<CompletableFuture<SystemTopicClient.Writer<PulsarEvent>>> writerFutures) {
        List<CompletableFuture<Void>> loggingFutures = new ArrayList<>();

        for (CompletableFuture<SystemTopicClient.Writer<PulsarEvent>> future : writerFutures) {
            loggingFutures.add(future.thenApply(writer -> {
                log.info("===> writer in mocked client create finished. topic: {}",
                        String.valueOf(writer.getSystemTopicClient().getTopicName()));
                return null;
            }));
        }
        return loggingFutures;
    }

    private static List<CompletableFuture<Void>> wrapTopicFutureWithLogging(
            List<CompletableFuture<Optional<Topic>>> topicFutures) {
        List<CompletableFuture<Void>> loggingFutures = new ArrayList<>();

        for (CompletableFuture<Optional<Topic>> future : topicFutures) {

            loggingFutures.add(future.thenApply(topicOptional -> {
                if (!topicOptional.isPresent()) {
                    return null;
                }
                log.info("===> topic {} create finished. topic: {}",
                        topicOptional.get().getName());
                return null;
            }));
        }
        return loggingFutures;
    }

    private static class MarkerTopicPoliciesSystemTopicClient extends TopicPoliciesSystemTopicClient {

        public static final List<CompletableFuture<Writer<PulsarEvent>>> writerCreateTaskHistory =
                Collections.synchronizedList(new ArrayList<>());

        public static final List<CompletableFuture<Reader<PulsarEvent>>> readerCreateTaskHistory =
                Collections.synchronizedList(new ArrayList<>());

        public MarkerTopicPoliciesSystemTopicClient(PulsarClient client, TopicName topicName) {
            super(client, topicName);
        }

        @Override
        protected CompletableFuture<Writer<PulsarEvent>> newWriterAsyncInternal() {
            CompletableFuture<Writer<PulsarEvent>> res = super.newWriterAsyncInternal();
            writerCreateTaskHistory.add(res);
            return res;
        }

        @Override
        protected CompletableFuture<Reader<PulsarEvent>> newReaderAsyncInternal() {
            CompletableFuture<Reader<PulsarEvent>> res = super.newReaderAsyncInternal();
            readerCreateTaskHistory.add(res);
            return res;
        }

        public static List<CompletableFuture<Reader<PulsarEvent>>> removeAllReaderCreateTasks() {
            List<CompletableFuture<Reader<PulsarEvent>>> tasks = new ArrayList<>();
            tasks.addAll(readerCreateTaskHistory);
            readerCreateTaskHistory.clear();
            return tasks;
        }

        public static List<CompletableFuture<Writer<PulsarEvent>>> removeAllWriterCreateTasks() {
            List<CompletableFuture<Writer<PulsarEvent>>> tasks = new ArrayList<>();
            tasks.addAll(writerCreateTaskHistory);
            writerCreateTaskHistory.clear();
            return tasks;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
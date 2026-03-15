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
package org.apache.pulsar.broker.transaction.buffer.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TableViewTest {

    @Test
    public void testFailedCreateReader() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        Function<TopicName, CompletableFuture<SystemTopicClient.Reader<Object>>> readerCreator = topicName -> {
            return FutureUtil.failedFuture(new PulsarClientException("test failed to create reader"));
        };
        TableView<Object> tableView = new TableView<>(readerCreator, 60000, executor);
        try {
            tableView.readLatest("public/default/tp");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Failed to create reader"));
        }
    }
}

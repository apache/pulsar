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
package org.apache.pulsar.functions.api.examples;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class AsyncContextFunction implements Function<String, CompletableFuture<Void>> {
    @Override
    public CompletableFuture<Void> process(String input, Context context) {
        Logger LOG = context.getLogger();
        CompletableFuture<Void> future = new CompletableFuture();

        // this method only delay a function execute.
        Executors.newCachedThreadPool().submit(() -> {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                LOG.error("Exception when Thread.sleep", e);
                future.completeExceptionally(e);
            }

            String inputTopics = context.getInputTopics().stream().collect(Collectors.joining(", "));
            String funcName = context.getFunctionName();

            String logMessage = String.format("A message with value of \"%s\" has arrived on " +
                    "one of the following topics: %s %n", input, inputTopics);
            LOG.info(logMessage);

            String metricName = String.format("function-%s-messages-received", funcName);
            context.recordMetric(metricName, 1);

            future.complete(null);
        });

        return future;
    }
}

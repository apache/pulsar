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
package org.apache.pulsar.client.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatternTopicsConsumerImpl extends TopicsConsumerImpl {
    private final Pattern topicsPattern;

    public PatternTopicsConsumerImpl(Pattern topicsPattern,
                              PulsarClientImpl client,
                              Collection<String> topics,
                              String subscription,
                              ConsumerConfiguration conf,
                              ExecutorService listenerExecutor,
                              CompletableFuture<Consumer> subscribeFuture) {
        super(client, topics, subscription, conf, listenerExecutor, subscribeFuture);
        this.topicsPattern = topicsPattern;
    }

    public Pattern getPattern() {
        return this.topicsPattern;
    }

    private static final Logger log = LoggerFactory.getLogger(PatternTopicsConsumerImpl.class);
}

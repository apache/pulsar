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


import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.CursorClient;
import org.apache.pulsar.client.api.CursorClientBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

public class CursorClientBuilderImpl implements CursorClientBuilder {

    private final PulsarClientImpl client;
    private String topic;

    public CursorClientBuilderImpl(PulsarClientImpl pulsarClient) {
        this.client = pulsarClient;
    }

    @Override
    public CursorClientBuilder topic(String topicName) {
        this.topic = topicName;
        return this;
    }

    @Override
    public CursorClient create() throws PulsarClientException {
        try {
            return createAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<CursorClient> createAsync() {
        return client.createCursorClientAsync(topic);
    }
}

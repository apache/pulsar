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
package org.apache.pulsar.connect.core;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Pulsar's Sink interface. Sink read data from
 * a Pulsar topic and write it to external sinks(kv store, database, filesystem ,etc)
 * The lifcycle of a Sink is to open it passing any config needed
 * by it to initialize(like open network connection, authenticate, etc).
 * On every message from the designated PulsarTopic, the write method is
 * invoked which writes the message to the external sink. One can use close
 * at the end of the session to do any cleanup
 */
public interface Sink<T> extends AutoCloseable {
    /**
     * Open connector with configuration
     *
     * @param config initialization config
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(final Map<String, String> config) throws Exception;

    /**
     * Attempt to publish a type safe collection of messages
     *
     * @param message Object to publish to the sink
     * @return Completable future fo async publish request
     */
    CompletableFuture<Void> write(final Message<T> message);
}
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
package org.apache.pulsar.client.api;


import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link ConsumerBuilder} is used to configure and create instances of {@link Consumer}.
 *
 * @see PulsarClient#newCursorClient()
 * @since 2.9.0
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface CursorClientBuilder {

    /**
     * Specify the topic this cursor client will connect to.
     *
     * @param topicName Topic name.
     * @return The builder instance
     */
    CursorClientBuilder topic(String topicName);

    /**
     * Finalize the {@link CursorClient} creation to the topic.
     *
     * @return The created {@link CursorClient} instance.
     * @throws PulsarClientException if the the create operation fails
     */
    CursorClient create() throws PulsarClientException;

    /**
     * Finalize the {@link CursorClient} creation to the topic in asynchronous mode.
     *
     * @return A future that will yield a {@link CursorClient} instance
     */
    CompletableFuture<CursorClient> createAsync();

}

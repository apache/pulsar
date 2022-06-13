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
import org.apache.pulsar.client.api.MessageId;

/**
 *
 */
public interface SendCallback {

    /**
     * invoked when send operation completes.
     *
     * @param e
     */
    void sendComplete(Exception e);

    /**
     * used to specify a callback to be invoked on completion of a send operation for individual messages sent in a
     * batch. Callbacks for messages in a batch get chained
     *
     * @param msg message sent
     * @param scb callback associated with the message
     */
    void addCallback(MessageImpl<?> msg, SendCallback scb);

    /**
     *
     * @return next callback in chain
     */
    SendCallback getNextSendCallback();

    /**
     * Return next message in chain.
     *
     * @return next message in chain
     */
    MessageImpl<?> getNextMessage();

    /**
     *
     * @return future associated with callback
     */
    CompletableFuture<MessageId> getFuture();
}

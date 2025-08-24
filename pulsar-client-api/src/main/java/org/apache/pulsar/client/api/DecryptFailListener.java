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
package org.apache.pulsar.client.api;

import java.io.Serializable;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;


/**
 * <p>This listener is invoked when receives an encrypted message and cannot be decrypted successfully,
 * either because no {@link CryptoKeyReader} is configured or the configured {@link CryptoKeyReader}
 * cannot decrypt the message. This allows applications to handle decryption failures separately
 * from normal message processing.
 *
 * <p>This listener must be used together with a {@link MessageListener} and cannot be used
 * with {@link ConsumerCryptoFailureAction}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface DecryptFailListener<T> extends Serializable {
    /**
     * This method is called whenever a new encrypted message is received and cannot be decrypted successfully
     * by {@link CryptoKeyReader}
     * <p>Messages are guaranteed to be delivered in order and from the same thread for a single consumer
     *
     * <p>This method will only be called once for each encrypted message.
     *
     * <p>Application is responsible for acknowledging the message by calling any of the consumer
     * acknowledgement methods if needed.
     *
     * <p>Application is responsible for handling any exception that could be thrown while
     * processing the undecryptable message.
     *
     * @param consumer the consumer that received the undecryptable message
     * @param msg the encrypted message object that failed decryption
     */
    void received(Consumer<T> consumer, Message<T> msg);
}

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

import java.util.List;
import java.util.Optional;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Result for one physical entry slot covered by a RandomReader read.
 *
 * @param <T> the message payload type
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RandomReadResult<T> {

    /**
     * @return the physical entry message id for this slot
     */
    MessageId getMessageId();

    /**
     * @return whether this entry is visible as client messages
     */
    boolean isVisible();

    /**
     * Return decoded messages for a visible entry.
     *
     * @throws PulsarClientException.MessageInvisibleException if this entry is invisible
     */
    List<Message<T>> getMessages() throws PulsarClientException.MessageInvisibleException;

    /**
     * @return the invisibility exception for an invisible entry, otherwise empty
     */
    Optional<PulsarClientException.MessageInvisibleException> getInvisibleException();
}

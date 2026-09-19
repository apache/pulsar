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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.common.api.proto.ScalableConsumerAssignment;

/**
 * Callback interface for a registered scalable-topic consumer. Implemented by the V5
 * client's per-consumer session. The broker pushes {@link CommandScalableTopicAssignmentUpdate}
 * messages tagged with the consumer's {@code consumerId}; {@link ClientCnx} dispatches
 * those to the matching session.
 */
public interface ScalableConsumerSession {

    /**
     * Called when the broker pushes a new segment assignment to this consumer (after a
     * peer joined/left the subscription or the topic layout changed).
     */
    void onAssignmentUpdate(ScalableConsumerAssignment assignment);

    /**
     * Called when the connection to the broker is closed. Implementations should mark
     * the session as needing reconnection and re-register on the next available
     * connection.
     */
    void connectionClosed();
}

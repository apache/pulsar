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

import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.ServerError;

/**
 * Callback interface for receiving DAG watch session updates from the broker.
 * Implemented by the v5 client's DagWatchClient.
 */
public interface DagWatchSession {

    /**
     * Called when the broker sends a DAG update for this session.
     */
    void onUpdate(ScalableTopicDAG dag);

    /**
     * Called when the broker sends an error for this session.
     */
    void onError(ServerError error, String message);

    /**
     * Called when the connection to the broker is closed.
     * Implementations should fail any pending operations and optionally reconnect.
     */
    void connectionClosed();
}

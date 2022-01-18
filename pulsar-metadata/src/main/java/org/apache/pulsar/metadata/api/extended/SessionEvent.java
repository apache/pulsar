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
package org.apache.pulsar.metadata.api.extended;

/**
 * An event regarding a session of MetadataStore.
 */
public enum SessionEvent {

    /**
     * The client is temporarily disconnected, although the session is still valid.
     */
    ConnectionLost,

    /**
     * The client was able to successfully reconnect.
     */
    Reconnected,

    /**
     * The session was lost, all the ephemeral keys created on the store within the current session might have been
     * already expired.
     */
    SessionLost,

    /**
     * The session was established.
     */
    SessionReestablished;

    /**
     * Check whether the state represents a connected or not-connected state.
     */
    public boolean isConnected() {
        switch (this) {
            case Reconnected:
            case SessionReestablished:
                return true;

            case ConnectionLost:
            case SessionLost:
            default:
                return false;
        }
    }
}

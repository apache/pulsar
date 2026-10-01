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
package org.apache.pulsar.client.api.v5.config;

/**
 * Access mode for a producer on a topic.
 */
public enum ProducerAccessMode {

    /**
     * Multiple producers can publish on the topic.
     */
    SHARED,

    /**
     * Only one producer is allowed on the topic. If another producer tries to connect, it will fail.
     */
    EXCLUSIVE,

    /**
     * Only one producer is allowed on the topic. If another producer tries to connect,
     * the existing producer is fenced and disconnected.
     */
    EXCLUSIVE_WITH_FENCING,

    /**
     * Only one producer is allowed on the topic. If another producer is already connected,
     * the new producer will wait until it can acquire exclusive access.
     */
    WAIT_FOR_EXCLUSIVE
}

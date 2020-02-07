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

/**
 * Controls the behavior when multiple producers with the same producerName connect to a topic.
 */
public enum ProducerGroupMode {
    /**
     * Only one producer can be active at any one point in time.
     *
     * <p>Producers trying to connect with the same producerName and topic will be rejected.
     */
    Exclusive,

    /**
     * Multiple producers can be active and producing in parallel.
     *
     * <p>This can be used in active/active producer scenarios, when deduplication needs to work across multiple
     * producers.
     */
    Parallel,
//
//    /**
//     * Concurrently connecting producer will be blocked until the active producer fails.
//     */
//    Failover,
}
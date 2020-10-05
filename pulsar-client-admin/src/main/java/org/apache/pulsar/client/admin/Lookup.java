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
package org.apache.pulsar.client.admin;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This is an interface class to allow using command line tool to quickly lookup the broker serving the topic.
 */
public interface Lookup {

    /**
     * Lookup a topic.
     *
     * @param topic
     * @return the broker URL that serves the topic
     */
    String lookupTopic(String topic) throws PulsarAdminException;

    /**
     * Lookup a topic asynchronously.
     *
     * @param topic
     * @return the broker URL that serves the topic
     */
    CompletableFuture<String> lookupTopicAsync(String topic);

    /**
     * Lookup a partitioned topic.
     *
     * @param topic
     * @return the broker URLs that serves the topic
     */
    Map<String, String> lookupPartitionedTopic(String topic) throws PulsarAdminException;


    /**
     * Lookup a partitioned topic.
     *
     * @param topic
     * @return the broker URLs that serves the topic
     */
    CompletableFuture<Map<String, String>> lookupPartitionedTopicAsync(String topic);

    /**
     * Get a bundle range of a topic.
     *
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    String getBundleRange(String topic) throws PulsarAdminException;

    /**
     * Get a bundle range of a topic asynchronously.
     *
     * @param topic
     * @return
     */
    CompletableFuture<String> getBundleRangeAsync(String topic);
}

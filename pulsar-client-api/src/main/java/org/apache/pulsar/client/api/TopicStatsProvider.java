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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.policies.data.TopicInternalStatsInfo;
import org.apache.pulsar.common.policies.data.TopicStatsInfo;

/**
 * Stats provider provides API to fetch topic's stats and internal-stats.
 */
@InterfaceAudience.Public
public interface TopicStatsProvider {

    /**
     * @return the topic stats
     * @throws PulsarClientException
     */
    TopicStatsInfo  getStats() throws PulsarClientException;

    /**
     * @return the topic stats asynchronously
     */
    CompletableFuture<TopicStatsInfo>  getStatsAsync();

    /**
     * @return the internal topic stats
     * @throws PulsarClientException
     */
    TopicInternalStatsInfo  getInternalStats() throws PulsarClientException;

    /**
     * @return the internal topic stats asynchronously
     */
    CompletableFuture<TopicInternalStatsInfo>  getInternalStatsAsync();
}

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
package org.apache.pulsar.broker.service;

import java.util.List;
import java.util.Map;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Murmur3_32Hash;

public interface StickyKeyConsumerSelector {

    int DEFAULT_RANGE_SIZE =  2 << 15;

    /**
     * Add a new consumer.
     *
     * @param consumer new consumer
     */
    void addConsumer(Consumer consumer) throws ConsumerAssignException;

    /**
     * Remove the consumer.
     * @param consumer consumer to be removed
     */
    void removeConsumer(Consumer consumer);

    /**
     * Select a consumer by sticky key.
     *
     * @param stickyKey sticky key
     * @return consumer
     */
    default Consumer select(byte[] stickyKey) {
        return select(makeStickyKeyHash(stickyKey));
    }

    static int makeStickyKeyHash(byte[] stickyKey) {
        return Murmur3_32Hash.getInstance().makeHash(stickyKey);
    }

    /**
     * Select a consumer by hash.
     *
     * @param hash hash corresponding to sticky key
     * @return consumer
     */
    Consumer select(int hash);

    /**
     * Get key hash ranges handled by each consumer.
     * @return A map where key is a consumer name and value is list of hash range it receiving message for.
     */
    Map<Consumer, List<Range>> getConsumerKeyHashRanges();
}

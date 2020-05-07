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

import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;

public interface StickyKeyConsumerSelector {

    int DEFAULT_RANGE_SIZE =  2 << 15;

    /**
     * Add a new consumer
     * @param consumer new consumer
     */
    void addConsumer(Consumer consumer) throws ConsumerAssignException;

    /**
     * Remove the consumer
     * @param consumer consumer to be removed
     */
    void removeConsumer(Consumer consumer);

    /**
     * Select a consumer by sticky key
     *
     * @param stickyKey sticky key
     * @return consumer
     */
    Consumer select(byte[] stickyKey);

    /**
     * Select a consumer by hash of the sticky they
     * @param keyHash hash of sticky key
     * @return
     */
    Consumer select(int keyHash);

    /**
     * Select a consumer by key hash range index.
     * @param index index of the key hash range
     * @return
     */
    Consumer selectByIndex(int index);

    int getRangeSize();
}

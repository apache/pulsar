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

package org.apache.pulsar.replicator.function.utils;

import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.replicator.function.ReplicatorTopicData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatorTopicDataSerDe implements SerDe<ReplicatorTopicData> {

    private static final ReplicatorTopicDataSerDe INSTANCE = new ReplicatorTopicDataSerDe();

    public static ReplicatorTopicDataSerDe instance() {
        return INSTANCE;
    }

    @Override
    public ReplicatorTopicData deserialize(byte[] input) {
        try {
            return ObjectMapperFactory.getThreadLocal().readValue(input, ReplicatorTopicData.class);
        } catch (Exception e) {
            String data = new String(input);
            log.error("Failed to deserialize input {}", data, e);
            throw new IllegalArgumentException("invalid topic data " + data, e);
        }
    }

    @Override
    public byte[] serialize(ReplicatorTopicData input) {
        try {
            return ObjectMapperFactory.getThreadLocal().writeValueAsBytes(input);
        } catch (Exception e) {
            log.error("Failed to serialize input {}", input, e);
            throw new IllegalArgumentException("invalid topic data " + input, e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTopicDataSerDe.class);
}

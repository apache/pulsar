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
package org.apache.kafka.clients.simple.consumer;

import java.util.Map;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;

public class PulsarOffsetResponse extends OffsetResponse {

    private final Map<TopicAndPartition, Long> offsetResoponse;

    public PulsarOffsetResponse(Map<TopicAndPartition, Long> offsetResoponse) {
        super(null);
        this.offsetResoponse = offsetResoponse;
    }

    @Override
    public long[] offsets(String topic, int partition) {
        Long offset = offsetResoponse.get(new TopicAndPartition(topic, partition));
        long[] offsets = { offset != null ? offset : 0 };
        return offsets;
    }

    @Override
    public boolean hasError() {
        return false;
    }

    @Override
    public short errorCode(String topic, int partition) {
        return ErrorMapping.NoError();
    }
}

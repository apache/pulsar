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

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;

public class PulsarOffsetRequest extends OffsetRequest {

    private final Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo;

    public PulsarOffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, short versionId,
            String clientId) {
        super(requestInfo, versionId, clientId);
        this.requestInfo = requestInfo;
    }

    public Map<TopicAndPartition, PartitionOffsetRequestInfo> getRequestInfo() {
        return requestInfo;
    }
}

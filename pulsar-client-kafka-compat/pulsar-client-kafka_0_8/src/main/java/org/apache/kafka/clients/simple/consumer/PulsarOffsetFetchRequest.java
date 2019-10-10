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

import java.util.List;

import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;

public class PulsarOffsetFetchRequest extends OffsetFetchRequest {
    protected final String groupId;
    protected final List<TopicAndPartition> requestInfo;

    public PulsarOffsetFetchRequest(String groupId, List<TopicAndPartition> requestInfo, short versionId,
            int correlationId, String clientId) {
        super(groupId, requestInfo, versionId, correlationId, clientId);
        this.groupId = groupId;
        this.requestInfo = requestInfo;
    }
}

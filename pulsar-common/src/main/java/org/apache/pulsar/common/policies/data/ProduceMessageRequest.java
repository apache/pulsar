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
package org.apache.pulsar.common.policies.data;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Hold necessary parameters for rest publish message attempt
 */
@Data
@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class ProduceMessageRequest {

    private long schemaVersion;

    // Base64 encoded serialized schemaInfo for key
    private String keySchema;

    // Base64 encoded serialized schemaInfo for value
    private String valueSchema;

    private List<RestProduceMessage> messages;

    private String producerName;

    /**
     * Holds information for single message to be published
     */
    @Data
    @AllArgsConstructor
    @Setter
    @Getter
    @NoArgsConstructor
    public static class RestProduceMessage {

        // Key of the message for routing policy.
        private String key;

        // Encoded message body.
        private String value;

        // The partition to publish the message to.
        private int partition;

        // The properties of the message.
        private Map<String, String> properties;

        // The event time of the message.
        private long eventTime;

        // The sequence id of the message.
        private long sequenceId;

        // The list of clusters to replicate.
        private List<String> replicationClusters;

        // The flag to disable replication.
        private boolean disableReplication;

        // Deliver the message only at or after the specified absolute timestamp.
        private long deliverAt;

        // Deliver the message only after the specified relative delay in milliseconds.
        private long deliverAfterMs;
    }
}

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
package org.apache.pulsar.websocket.data;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.api.proto.CompressionType;

/**
 * Class represent single message to be published.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProducerMessage {
    // Actual message payload.
    private String payload;

    // Optional properties.
    private Map<String, String> properties;

    // Request context
    private String context;

    // Partition key.
    private String key;

    // Clusters to replicate message to.
    private List<String> replicationClusters;

    // Message event time.
    private String eventTime;

    // Message sequenceId.
    private long sequenceId;

    // Whether to disable replication of the message.
    private boolean disableReplication;

    // Deliver the message only at or after the specified absolute timestamp.
    private long deliverAt;

    // Deliver the message only after the specified relative delay in milliseconds.
    private long deliverAfterMs;

    // Version of schema to use for the message.
    private long schemaVersion;

    // Base64 encoded serialized schema for key
    private String keySchema;

    // Base64 encoded serialized schema for payload
    private String valueSchema;

    // Base64 encoded serialized initialization vector used when the client encrypts.
    private String encryptionParam;

    // Compression type. Do not set it if compression is not performed.
    private CompressionType compressionType;

    // The size of the payload before compression. Do not set it if compression is not performed.
    private Integer uncompressedMessageSize;
}

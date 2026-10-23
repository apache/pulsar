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
package org.apache.pulsar.metadata.api;

import java.util.HashSet;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.pulsar.metadata.api.extended.CreateOption;

/**
 * Metadata event used by {@link MetadataEventSynchronizer}.
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MetadataEvent {
    private String path;
    private byte[] value;
    private HashSet<CreateOption> options;
    private Long expectedVersion;
    private long lastUpdatedTimestamp;
    private String sourceCluster;
    private NotificationType type;
    private int version;
    public MetadataEvent(String path, byte[] value, HashSet<CreateOption> options, Long expectedVersion,
            long lastUpdatedTimestamp, String sourceCluster, NotificationType type) {
        super();
        this.path = path;
        this.value = value;
        this.options = options;
        this.expectedVersion = expectedVersion;
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
        this.sourceCluster = sourceCluster;
        this.type = type;
    }

}

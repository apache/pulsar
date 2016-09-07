/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;

import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.partition.PartitionedTopicMetadata;

public class PartitionMetadataLookupService {

    private final HttpClient httpClient;

    public PartitionMetadataLookupService(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(DestinationName destination) {
        return httpClient.get(String.format("admin/%s/partitions", destination.getLookupName()),
                PartitionedTopicMetadata.class);
    }
};
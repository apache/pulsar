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
package org.apache.pulsar.proxy.server;

import io.prometheus.client.Counter;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;

public interface LookupProxyHandler {

    Counter LOOKUP_REQUESTS = Counter
            .build("pulsar_proxy_lookup_requests", "Counter of topic lookup requests").create().register();

    Counter PARTITIONS_METADATA_REQUESTS = Counter
            .build("pulsar_proxy_partitions_metadata_requests", "Counter of partitions metadata requests").create()
            .register();

    Counter GET_TOPICS_OF_NAMESPACE_REQUESTS = Counter
            .build("pulsar_proxy_get_topics_of_namespace_requests", "Counter of getTopicsOfNamespace requests")
            .create()
            .register();

    Counter GET_SCHEMA_REQUESTS = Counter
            .build("pulsar_proxy_get_schema_requests", "Counter of schema requests")
            .create()
            .register();

    Counter REJECTED_LOOKUP_REQUESTS = Counter.build("pulsar_proxy_rejected_lookup_requests",
            "Counter of topic lookup requests rejected due to throttling").create().register();

    Counter REJECTED_PARTITIONS_METADATA_REQUESTS = Counter
            .build("pulsar_proxy_rejected_partitions_metadata_requests",
                    "Counter of partitions metadata requests rejected due to throttling")
            .create().register();

    Counter REJECTED_GET_TOPICS_OF_NAMESPACE_REQUESTS = Counter
            .build("pulsar_proxy_rejected_get_topics_of_namespace_requests",
                    "Counter of getTopicsOfNamespace requests rejected due to throttling")
            .create().register();

    void initialize(ProxyService proxy, ProxyConnection proxyConnection);

    void handleLookup(CommandLookupTopic lookup);

    void handlePartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata);

    void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace);

    void handleGetSchema(CommandGetSchema commandGetSchema);

}

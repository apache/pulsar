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
package org.apache.pulsar.client.impl.schema.generic;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Multi version generic schema provider by guava cache.
 */
public class MultiVersionSchemaInfoProvider implements SchemaInfoProvider {

    private static final Logger LOG = LoggerFactory.getLogger(MultiVersionSchemaInfoProvider.class);

    private final TopicName topicName;
    private final PulsarClientImpl pulsarClient;

    private final LoadingCache<BytesSchemaVersion, CompletableFuture<SchemaInfo>> cache = CacheBuilder.newBuilder()
        .maximumSize(100000)
        .expireAfterAccess(30, TimeUnit.MINUTES)
        .build(new CacheLoader<BytesSchemaVersion, CompletableFuture<SchemaInfo>>() {
            @Override
            public CompletableFuture<SchemaInfo> load(BytesSchemaVersion schemaVersion) {
                CompletableFuture<SchemaInfo> siFuture = loadSchema(schemaVersion.get());
                siFuture.whenComplete((si, cause) -> {
                    if (null != cause) {
                        cache.asMap().remove(schemaVersion, siFuture);
                    }
                });
                return siFuture;
            }
        });

    public MultiVersionSchemaInfoProvider(TopicName topicName, PulsarClientImpl pulsarClient) {
        this.topicName = topicName;
        this.pulsarClient = pulsarClient;
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
        try {
            if (null == schemaVersion) {
                return CompletableFuture.completedFuture(null);
            }
            return cache.get(BytesSchemaVersion.of(schemaVersion));
        } catch (ExecutionException e) {
            LOG.error("Can't get schema for topic {} schema version {}",
                    topicName.toString(), new String(schemaVersion, StandardCharsets.UTF_8), e);
            return FutureUtil.failedFuture(e.getCause());
        }
    }

    @Override
    public CompletableFuture<SchemaInfo> getLatestSchema() {
        return pulsarClient.getLookup()
            .getSchema(topicName)
            .thenApply(o -> o.orElse(null));
    }

    @Override
    public String getTopicName() {
        return topicName.getLocalName();
    }

    private CompletableFuture<SchemaInfo> loadSchema(byte[] schemaVersion) {
        return pulsarClient.getLookup()
                .getSchema(topicName, schemaVersion)
                .thenApply(o -> o.orElse(null));
    }

    public PulsarClientImpl getPulsarClient() {
        return pulsarClient;
    }
}

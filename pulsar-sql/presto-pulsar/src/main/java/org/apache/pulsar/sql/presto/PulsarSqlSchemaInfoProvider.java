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
package org.apache.pulsar.sql.presto;

import static java.util.concurrent.CompletableFuture.completedFuture;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.glassfish.jersey.internal.inject.InjectionManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi version schema info provider for Pulsar SQL leverage guava cache.
 */
public class PulsarSqlSchemaInfoProvider implements SchemaInfoProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSqlSchemaInfoProvider.class);

    private final TopicName topicName;

    private final PulsarAdmin pulsarAdmin;

    private final LoadingCache<BytesSchemaVersion, CompletableFuture<SchemaInfo>> cache = CacheBuilder.newBuilder()
            .maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Nonnull
                @Override
                public CompletableFuture<SchemaInfo> load(@Nonnull BytesSchemaVersion schemaVersion) {
                    return loadSchema(schemaVersion);
                }
            });

    public PulsarSqlSchemaInfoProvider(TopicName topicName, PulsarAdmin pulsarAdmin) {
        this.topicName = topicName;
        this.pulsarAdmin = pulsarAdmin;
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
        try {
            if (null == schemaVersion) {
                return completedFuture(null);
            }
            return cache.get(BytesSchemaVersion.of(schemaVersion));
        } catch (ExecutionException e) {
            LOG.error("Can't get generic schema for topic %s schema version %s",
                    topicName.toString(), new String(schemaVersion, StandardCharsets.UTF_8), e);
            return FutureUtil.failedFuture(e.getCause());
        }
    }

    @Override
    public CompletableFuture<SchemaInfo> getLatestSchema() {
        return pulsarAdmin.schemas().getSchemaInfoAsync(topicName.toString());
    }

    @Override
    public String getTopicName() {
        return topicName.getLocalName();
    }

    private CompletableFuture<SchemaInfo> loadSchema(BytesSchemaVersion bytesSchemaVersion) {
        ClassLoader originalContextLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(InjectionManagerFactory.class.getClassLoader());
            long version = ByteBuffer.wrap(bytesSchemaVersion.get()).getLong();
            return pulsarAdmin.schemas().getSchemaInfoAsync(topicName.toString(), version);
        } finally {
            Thread.currentThread().setContextClassLoader(originalContextLoader);
        }
    }


    public static SchemaInfo defaultSchema() {
        return Schema.BYTES.getSchemaInfo();
    }

}
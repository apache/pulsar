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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Multi version generic schema provider by guava cache.
 */
public class MultiVersionGenericSchemaProvider implements SchemaProvider<GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiVersionGenericSchemaProvider.class);

    private final TopicName topicName;
    private final PulsarClientImpl pulsarClient;

    private final LoadingCache<byte[], GenericSchema> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<byte[], GenericSchema>() {
                @Override
                public GenericSchema load(byte[] schemaVersion) throws Exception {
                    return loadSchema(schemaVersion);
                }
            });

    public MultiVersionGenericSchemaProvider(TopicName topicName, PulsarClientImpl pulsarClient) {
        this.topicName = topicName;
        this.pulsarClient = pulsarClient;
    }

    @Override
    public GenericSchema getSchema(byte[] schemaVersion) {
        try {
            if (null == schemaVersion) {
                return null;
            }
            return cache.get(schemaVersion);
        } catch (ExecutionException e) {
            LOG.error("Can't get generic schema for topic {} schema version {}",
                    topicName.toString(), new String(schemaVersion, StandardCharsets.UTF_8), e);
            return null;
        }
    }

    private GenericSchema loadSchema(byte[] schemaVersion) throws ExecutionException, InterruptedException {
        Optional<SchemaInfo> schemaInfo = pulsarClient.getLookup()
                .getSchema(topicName, schemaVersion).get();
        return schemaInfo.map(GenericSchemaImpl::of).orElse(null);
    }

    public TopicName getTopic() {
        return topicName;
    }

    public PulsarClientImpl getPulsarClient() {
        return pulsarClient;
    }
}

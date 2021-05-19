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
package org.apache.pulsar.client.impl.schema.reader;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.AvroTypeException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.SerializationException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * The multi version reader abstract class, implement it will handle the multi version schema.
 */
public abstract class AbstractMultiVersionReader<T> implements SchemaReader<T> {

    protected final SchemaReader<T> providerSchemaReader;
    protected SchemaInfoProvider schemaInfoProvider;

    LoadingCache<BytesSchemaVersion, SchemaReader<T>> readerCache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<BytesSchemaVersion, SchemaReader<T>>() {
                @Override
                public SchemaReader<T> load(BytesSchemaVersion schemaVersion) {
                    return loadReader(schemaVersion);
                }
            });

    public AbstractMultiVersionReader(SchemaReader<T> providerSchemaReader) {
        this.providerSchemaReader = providerSchemaReader;
    }

    @Override
    public T read(byte[] bytes, int offset, int length) {
        return providerSchemaReader.read(bytes);
    }

    @Override
    public T read(InputStream inputStream) {
        return providerSchemaReader.read(inputStream);
    }

    @Override
    public T read(InputStream inputStream, byte[] schemaVersion) {
        try {
            return schemaVersion == null ? read(inputStream) :
                    getSchemaReader(schemaVersion).read(inputStream);
        } catch (ExecutionException e) {
            LOG.error("Can't get generic schema for topic {} schema version {}",
                    schemaInfoProvider.getTopicName(), Hex.encodeHexString(schemaVersion), e);
            throw new RuntimeException("Can't get generic schema for topic " + schemaInfoProvider.getTopicName());
        }
    }

    public SchemaReader<T> getSchemaReader(byte[] schemaVersion) throws ExecutionException {
        return readerCache.get(BytesSchemaVersion.of(schemaVersion));
    }

    @Override
    public T read(byte[] bytes, byte[] schemaVersion) {
        try {
            return schemaVersion == null ? read(bytes) :
                    getSchemaReader(schemaVersion).read(bytes);
        } catch (ExecutionException | AvroTypeException e) {
            if (e instanceof AvroTypeException) {
                throw new SchemaSerializationException(e);
            }
            LOG.error("Can't get generic schema for topic {} schema version {}",
                    schemaInfoProvider.getTopicName(), Hex.encodeHexString(schemaVersion), e);
            throw new RuntimeException("Can't get generic schema for topic " + schemaInfoProvider.getTopicName());
        }
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        this.schemaInfoProvider = schemaInfoProvider;
    }

    /**
     * Load the schema reader for reading messages encoded by the given schema version.
     *
     * @param schemaVersion the provided schema version
     * @return the schema reader for decoding messages encoded by the provided schema version.
     */
    protected abstract SchemaReader<T> loadReader(BytesSchemaVersion schemaVersion);

    /**
     * TODO: think about how to make this async
     */
    protected SchemaInfo getSchemaInfoByVersion(byte[] schemaVersion) {
        try {
            return schemaInfoProvider.getSchemaByVersion(schemaVersion).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SerializationException(
                    "Interrupted at fetching schema info for " + SchemaUtils.getStringSchemaVersion(schemaVersion),
                    e
            );
        } catch (ExecutionException e) {
            throw new SerializationException(
                    "Failed at fetching schema info for " + SchemaUtils.getStringSchemaVersion(schemaVersion),
                    e.getCause()
            );
        }
    }

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractMultiVersionReader.class);
}

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
package org.apache.pulsar.client.impl.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.Schema.Parser;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a base schema implementation for `Struct` types.
 * A struct type is used for presenting records (objects) which
 * have multiple fields.
 *
 * <p>Currently Pulsar supports 3 `Struct` types -
 * {@link org.apache.pulsar.common.schema.SchemaType#AVRO},
 * {@link org.apache.pulsar.common.schema.SchemaType#JSON},
 * and {@link org.apache.pulsar.common.schema.SchemaType#PROTOBUF}.
 */
public abstract class StructSchema<T> implements Schema<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(StructSchema.class);

    protected final org.apache.avro.Schema schema;
    protected final SchemaInfo schemaInfo;
    protected SchemaReader<T> reader;
    protected SchemaWriter<T> writer;
    protected SchemaInfoProvider schemaInfoProvider;
    private final LoadingCache<byte[], SchemaReader<T>> readerCache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<byte[], SchemaReader<T>>() {
                @Override
                public SchemaReader<T> load(byte[] schemaVersion) {
                    return loadReader(schemaVersion);
                }
            });

    protected StructSchema(SchemaInfo schemaInfo) {
        this.schema = parseAvroSchema(new String(schemaInfo.getSchema(), UTF_8));
        this.schemaInfo = schemaInfo;
    }

    public org.apache.avro.Schema getAvroSchema() {
        return schema;
    }

    @Override
    public byte[] encode(T message) {
        return writer.write(message);
    }

    @Override
    public T decode(byte[] bytes) {
        return reader.read(bytes);
    }

    @Override
    public T decode(byte[] bytes, byte[] schemaVersion) {
        try {
            return readerCache.get(schemaVersion).read(bytes);
        } catch (ExecutionException e) {
            LOG.error("Can't get generic schema for topic {} schema version {}",
                    schemaInfoProvider.getTopicName(), Hex.encodeHexString(schemaVersion), e);
            throw new RuntimeException("Can't get generic schema for topic " + schemaInfoProvider.getTopicName());
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }

    protected static org.apache.avro.Schema createAvroSchema(SchemaDefinition schemaDefinition) {
        Class pojo = schemaDefinition.getPojo();

        if (StringUtils.isNotBlank(schemaDefinition.getJsonDef())) {
            return parseAvroSchema(schemaDefinition.getJsonDef());
        } else if (pojo != null) {
            return schemaDefinition.getAlwaysAllowNull() ? ReflectData.AllowNull.get().getSchema(pojo) : ReflectData.get().getSchema(pojo);
        } else {
            throw new RuntimeException("Schema definition must specify pojo class or schema json definition");
        }
    }

    protected static org.apache.avro.Schema parseAvroSchema(String schemaJson) {
        final Parser parser = new Parser();
        return parser.parse(schemaJson);
    }

    protected static <T> SchemaInfo parseSchemaInfo(SchemaDefinition<T> schemaDefinition, SchemaType schemaType) {
        return SchemaInfo.builder()
                .schema(createAvroSchema(schemaDefinition).toString().getBytes(UTF_8))
                .properties(schemaDefinition.getProperties())
                .name("")
                .type(schemaType).build();
    }

    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        this.schemaInfoProvider = schemaInfoProvider;
    }

    /**
     * Load the schema reader for reading messages encoded by the given schema version.
     *
     * @param schemaVersion the provided schema version
     * @return the schema reader for decoding messages encoded by the provided schema version.
     */
    protected abstract SchemaReader<T> loadReader(byte[] schemaVersion);

    protected void setWriter(SchemaWriter<T> writer) {
        this.writer = writer;
    }

    protected void setReader(SchemaReader<T> reader) {
        this.reader = reader;
    }

    protected SchemaReader<T> getReader() {
        return  reader;
    }

}

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.Schema.Parser;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaProvider;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionGenericSchemaProvider;
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
abstract class StructSchema<T> implements Schema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(StructSchema.class);

    protected final org.apache.avro.Schema schema;
    protected final SchemaInfo schemaInfo;
    protected final SchemaReader<T> reader;
    protected final SchemaWriter<T> writer;
    private boolean supportSchemaVersioning;
    protected SchemaProvider schemaProvider;
    private final LoadingCache<byte[], SchemaReader<T>> readerCache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<byte[], SchemaReader<T>>() {
                @Override
                public SchemaReader<T> load(byte[] schemaVersion) throws Exception {
                    return loadReader(schemaVersion);
                }
            });

    protected StructSchema(SchemaType schemaType,
                           org.apache.avro.Schema schema,
                           SchemaDefinition schemaDefinition,
                           SchemaWriter<T> writer,
                           SchemaReader<T> reader) {
        this.schema = schema;
        this.schemaInfo = new SchemaInfo();
        this.schemaInfo.setName("");
        this.schemaInfo.setType(schemaType);
        this.schemaInfo.setSchema(this.schema.toString().getBytes(UTF_8));
        this.schemaInfo.setProperties(schemaDefinition.getProperties());
        this.supportSchemaVersioning = schemaDefinition.getSupportSchemaVersioning();
        this.writer = writer;
        this.reader = reader;
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
                    ((MultiVersionGenericSchemaProvider)schemaProvider).getTopic().toString(), new String(schemaVersion, StandardCharsets.UTF_8), e);
            return null;
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }

    protected static org.apache.avro.Schema createAvroSchema(SchemaDefinition schemaDefinition) {
        Class pojo = schemaDefinition.getPojo();
        return schemaDefinition.getAlwaysAllowNull() ? ReflectData.AllowNull.get().getSchema(pojo) : ReflectData.get().getSchema(pojo);
    }

    protected static org.apache.avro.Schema parseAvroSchema(String jsonDef) {
        Parser parser = new Parser();
        return parser.parse(jsonDef);
    }

    public boolean supportSchemaVersioning() {
        return supportSchemaVersioning;
    }

    public void setSchemaProvider(SchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }

    protected abstract SchemaReader loadReader(byte[] schemaVersion);

}

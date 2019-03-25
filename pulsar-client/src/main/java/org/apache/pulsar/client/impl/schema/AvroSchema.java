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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionGenericSchemaProvider;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An AVRO schema implementation.
 */
@Slf4j
public class AvroSchema<T> extends StructSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchema.class);

    private ReflectDatumWriter<T> datumWriter;
    private ReflectDatumReader<T> datumReader;
    private BinaryEncoder encoder;
    private ByteArrayOutputStream byteArrayOutputStream;
    private static final ThreadLocal<BinaryDecoder> decoders =
            new ThreadLocal<>();
    private boolean supportSchemaVersioning;
    private final LoadingCache<byte[], ReflectDatumReader<T>> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<byte[], ReflectDatumReader<T>>() {
                @Override
                public ReflectDatumReader<T> load(byte[] schemaVersion) throws Exception {
                    return loadReader(schemaVersion);
                }
            });
    private AvroSchema(org.apache.avro.Schema schema,
                       SchemaDefinition schemaDefinition) {
        super(
            SchemaType.AVRO,
            schema,
            schemaDefinition.getProperties());
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, this.encoder);
        this.datumWriter = new ReflectDatumWriter<>(schema);
        this.datumReader = new ReflectDatumReader<>(schema);
        this.supportSchemaVersioning = schemaDefinition.getSupportSchemaVersioning();
    }

    @Override
    public synchronized byte[] encode(T message) {
        try {
            datumWriter.write(message, this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            BinaryDecoder decoderFromCache = decoders.get();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, decoderFromCache);
            if (decoderFromCache == null) {
                decoders.set(decoder);
            }
            return datumReader.read(null, DecoderFactory.get().binaryDecoder(bytes, decoder));
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public T decode(byte[] bytes, byte[] schemaVersion) {
        try {
            BinaryDecoder decoderFromCache = decoders.get();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, decoderFromCache);
            if (decoderFromCache == null) {
                decoders.set(decoder);
            }
            return cache.get(schemaVersion).read(null, DecoderFactory.get().binaryDecoder(bytes, decoder));
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
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

    public static <T> AvroSchema<T> of(SchemaDefinition<T> schemaDefinition) {
        return schemaDefinition.getJsonDef() == null ?
                new AvroSchema<>(createAvroSchema(schemaDefinition), schemaDefinition) : new AvroSchema<>(parseAvroSchema(schemaDefinition.getJsonDef()), schemaDefinition);
    }

    public static <T> AvroSchema<T> of(Class<T> pojo) {
        return AvroSchema.of(SchemaDefinition.<T>builder().withPojo(pojo).build());
    }

    public static <T> AvroSchema<T> of(Class<T> pojo, Map<String, String> properties) {
        SchemaDefinition<T> schemaDefinition = SchemaDefinition.<T>builder().withPojo(pojo).withProperties(properties).build();
        return new AvroSchema<>(createAvroSchema(schemaDefinition), schemaDefinition);
    }

    public boolean supportSchemaVersioning(){
        return supportSchemaVersioning;
    }

    private ReflectDatumReader loadReader(byte[] schemaVersion)  {
        return new ReflectDatumReader<T>(((GenericAvroSchema)schemaProvider.getVersionSchema(schemaVersion)).getAvroSchema(),schema);
    }

}

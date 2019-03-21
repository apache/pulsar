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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionGenericSchemaProvider;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An MultiversionSchema.
 */
@Slf4j
public class MultiVersionSchema<T> implements Schema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiVersionSchema.class);

    private BinaryEncoder encoder;
    private ByteArrayOutputStream byteArrayOutputStream;
    private final MultiVersionGenericSchemaProvider provider;
    private org.apache.avro.Schema schema;
    private static final ThreadLocal<BinaryDecoder> decoders =
            new ThreadLocal<>();
    private final LoadingCache<byte[], ReflectDatumReader<T>> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<byte[], ReflectDatumReader<T>>() {
                @Override
                public ReflectDatumReader<T> load(byte[] schemaVersion) throws Exception {
                    return loadReader(schemaVersion);
                }
            });
    public MultiVersionSchema(org.apache.avro.Schema schema,
                               MultiVersionGenericSchemaProvider provider) {
        this.provider = provider;
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, this.encoder);
        this.schema = schema;
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
                    provider.getTopic().toString(), new String(schemaVersion, StandardCharsets.UTF_8), e);
            return null;
        }
    }

    private ReflectDatumReader loadReader(byte[] schemaVersion) throws ExecutionException, InterruptedException {
        return new ReflectDatumReader<T>(((GenericAvroSchema)provider.getSchema(schemaVersion)).getAvroSchema(),schema);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return null;
    }

    @Override
    public void validate(byte[] message) {

    }

    @Override
    public byte[] encode(Object message) {
        return new byte[0];
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

}

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

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class AvroReader<T> implements SchemaReader<T> {

    private ReflectDatumReader<T> reader;
    private static final ThreadLocal<BinaryDecoder> decoders =
            new ThreadLocal<>();

    public AvroReader(Schema schema) {
        this.reader = new ReflectDatumReader<>(schema);
    }

    public AvroReader(Schema schema, ClassLoader classLoader) {
        log.info("AvroRaeader2 - classLoader: {}", classLoader);
        if (classLoader != null) {
            this.reader = new ReflectDatumReader<>(schema, schema, new ReflectData(classLoader));
        } else {
            this.reader = new ReflectDatumReader<>(schema);
        }
    }

    public AvroReader(Schema writerSchema, Schema readerSchema, ClassLoader classLoader) {
        log.info("AvroRaeader3 - classLoader: {}", classLoader);
        if (classLoader != null) {
            this.reader = new ReflectDatumReader<>(writerSchema, readerSchema, new ReflectData(classLoader));
        } else {
            this.reader = new ReflectDatumReader<>(writerSchema, readerSchema);
        }
    }

    @Override
    public T read(byte[] bytes, int offset, int length) {
        try {
            log.info("AvroReader read bytes - classLoader: {}", reader.getSpecificData().getClassLoader());
            BinaryDecoder decoderFromCache = decoders.get();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, offset, length, decoderFromCache);
            if (decoderFromCache == null) {
                decoders.set(decoder);
            }
            return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, offset, length, decoder));
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public T read(InputStream inputStream) {
        try {
            log.info("AvroReader read inputStream - classLoader: {}", reader.getSpecificData().getClassLoader());
            BinaryDecoder decoderFromCache = decoders.get();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, decoderFromCache);
            if (decoderFromCache == null) {
                decoders.set(decoder);
            }
            return reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, decoder));
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error("AvroReader close inputStream close error", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(AvroReader.class);

}

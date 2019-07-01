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
package org.apache.pulsar.sql.presto;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class AvroSchemaHandler implements SchemaHandler {

    private final List<PulsarColumnHandle> columnHandles;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final DatumReader<GenericRecord> datumReader;

    private final TopicName topicName;

    private final Schema schema;

    private static final FastThreadLocal<BinaryDecoder> decoders =
            new FastThreadLocal<>();

    private final LoadingCache<Long, GenericDatumReader<GenericRecord>> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<Long, GenericDatumReader<GenericRecord>>() {
                @Override
                public GenericDatumReader<GenericRecord> load(Long schemaVersion) throws Exception {
                    return getReader(schemaVersion);
                }
            });

    private GenericDatumReader<GenericRecord> getReader(Long schemaVersion) throws PulsarClientException, PulsarAdminException {

        return new GenericDatumReader<>(parseAvroSchema(pulsarConnectorConfig.getPulsarAdmin()
                .schemas()
                .getSchemaInfo(topicName.toString(), schemaVersion)
                .getSchemaDefinition()), schema);
    }

    private static final Logger log = Logger.get(AvroSchemaHandler.class);

    public AvroSchemaHandler(Schema schema, List<PulsarColumnHandle> columnHandles, PulsarConnectorConfig pulsarConnectorConfig, TopicName topicName) {
        this.datumReader = new GenericDatumReader<>(schema);
        this.schema = schema;
        this.columnHandles = columnHandles;
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        this.topicName = topicName;
    }

    @Override
    public Object deserialize(RawMessage rawMessage) {

        ByteBuf payload = rawMessage.getData();
        ByteBuf heapBuffer = null;
        try {
            BinaryDecoder decoderFromCache = decoders.get();

            // Make a copy into a heap buffer, since Avro cannot deserialize directly from direct memory
            int size = payload.readableBytes();
            heapBuffer = ByteBufAllocator.DEFAULT.heapBuffer(size, size);
            heapBuffer.writeBytes(payload);

            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(heapBuffer.array(), heapBuffer.arrayOffset(),
                    heapBuffer.readableBytes(), decoderFromCache);
            if (decoderFromCache==null) {
                decoders.set(decoder);
            }
            byte[] schemaVersionBytes = rawMessage.getSchemaVersion();
            if (schemaVersionBytes != null && schemaVersionBytes.length >= 8) {
                return cache.get(bytes2Long(schemaVersionBytes)).read(null, decoder);
            } else {
                return datumReader.read(null, decoder);
            }
        } catch (IOException | ExecutionException e) {
            if (e instanceof  ExecutionException) {
                log.error("Can't get generic schema for topic {} schema version {}",
                        topicName, rawMessage.getSchemaVersion(), e);
            }
            log.error(e);
        } finally {
            ReferenceCountUtil.safeRelease(heapBuffer);
        }
        return null;
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        try {
            GenericRecord record = (GenericRecord) currentRecord;
            PulsarColumnHandle pulsarColumnHandle = this.columnHandles.get(index);
            Integer[] positionIndices = pulsarColumnHandle.getPositionIndices();
            Object curr = record.get(positionIndices[0]);
            if (curr == null) {
                return null;
            }
            if (positionIndices.length > 0) {
                for (int i = 1 ; i < positionIndices.length; i++) {
                    curr = ((GenericRecord) curr).get(positionIndices[i]);
                    if (curr == null) {
                        return null;
                    }
                }
            }
            return curr;
        } catch (Exception ex) {
            log.debug(ex,"%s", ex);
        }
        return null;
    }

    protected static org.apache.avro.Schema parseAvroSchema(String schemaJson) {
        final Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaJson);
    }

    public static long bytes2Long(byte[] byteNum) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }
}

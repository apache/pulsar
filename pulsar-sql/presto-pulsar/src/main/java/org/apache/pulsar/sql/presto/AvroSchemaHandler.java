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

import io.airlift.log.Logger;
import org.apache.pulsar.shade.io.netty.util.concurrent.FastThreadLocal;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.List;

public class AvroSchemaHandler implements SchemaHandler {

    private final DatumReader<GenericRecord> datumReader;

    private final List<PulsarColumnHandle> columnHandles;

    public static final FastThreadLocal<BinaryDecoder> decoders =
            new FastThreadLocal<>();

    private static final Logger log = Logger.get(AvroSchemaHandler.class);

    public AvroSchemaHandler(Schema schema, List<PulsarColumnHandle> columnHandles) {
        this.datumReader = new GenericDatumReader<>(schema);
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(byte[] bytes) {
        try {
            BinaryDecoder decoderFromCache = decoders.get();
            BinaryDecoder decoder=DecoderFactory.get().binaryDecoder(bytes, decoderFromCache);
            if (decoderFromCache==null) {
                decoders.set(decoder);
            }
            return this.datumReader.read(null, decoder);
        } catch (IOException e) {
            log.error(e);
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
}

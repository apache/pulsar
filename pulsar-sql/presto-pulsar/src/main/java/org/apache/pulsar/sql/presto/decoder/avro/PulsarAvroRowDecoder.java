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
package org.apache.pulsar.sql.presto.decoder.avro;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Refer to {@link io.prestosql.decoder.avro.AvroRowDecoder}
 */
public class PulsarAvroRowDecoder implements RowDecoder {

    public static final String NAME = "avro";
    private final DatumReader<GenericRecord> avroRecordReader;
    private final Map<DecoderColumnHandle, PulsarAvroColumnDecoder> columnDecoders;

    public PulsarAvroRowDecoder(DatumReader<GenericRecord> avroRecordReader, Set<DecoderColumnHandle> columns) {
        this.avroRecordReader = requireNonNull(avroRecordReader, "avroRecordReader is null");
        requireNonNull(columns, "columns is null");
        columnDecoders = columns.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private PulsarAvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle) {
        return new PulsarAvroColumnDecoder(columnHandle);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, @Nullable Map<String, String> dataMap) {
        GenericRecord avroRecord;
        try {
            avroRecord = avroRecordReader.read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null));
        } catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed.", e);
        }
        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(avroRecord))));
    }


}

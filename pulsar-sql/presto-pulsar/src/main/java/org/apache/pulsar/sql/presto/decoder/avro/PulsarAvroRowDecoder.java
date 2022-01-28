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

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import io.netty.buffer.ByteBuf;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.sql.presto.PulsarRowDecoder;

/**
 * Refer to {@link io.prestosql.decoder.avro.AvroRowDecoder}.
 */
public class PulsarAvroRowDecoder implements PulsarRowDecoder {

    private final GenericAvroSchema genericAvroSchema;
    private final Map<DecoderColumnHandle, PulsarAvroColumnDecoder> columnDecoders;

    public PulsarAvroRowDecoder(GenericAvroSchema genericAvroSchema, Set<DecoderColumnHandle> columns) {
        this.genericAvroSchema = requireNonNull(genericAvroSchema, "genericAvroSchema is null");
        columnDecoders = columns.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private PulsarAvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle) {
        return new PulsarAvroColumnDecoder(columnHandle);
    }

    /**
     * decode ByteBuf by {@link org.apache.pulsar.client.api.schema.GenericSchema}.
     * @param byteBuf
     * @return
     */
    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf) {
        GenericRecord avroRecord;
        try {
            GenericAvroRecord record = (GenericAvroRecord) genericAvroSchema.decode(byteBuf);
            avroRecord = record.getAvroRecord();
        } catch (Exception e) {
            e.printStackTrace();
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding avro record failed.", e);
        }
        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(avroRecord))));
    }
}

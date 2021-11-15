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
package org.apache.pulsar.sql.presto.decoder.primitive;

import static io.prestosql.decoder.FieldValueProviders.booleanValueProvider;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;
import static org.apache.pulsar.sql.presto.PulsarFieldValueProviders.doubleValueProvider;

import io.netty.buffer.ByteBuf;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.FieldValueProviders;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.client.impl.schema.AbstractSchema;
import org.apache.pulsar.sql.presto.PulsarRowDecoder;

/**
 * Primitive Schema PulsarRowDecoder.
 */
public class PulsarPrimitiveRowDecoder implements PulsarRowDecoder {

    private final DecoderColumnHandle columnHandle;
    private AbstractSchema schema;

    public PulsarPrimitiveRowDecoder(AbstractSchema schema, DecoderColumnHandle column) {
        this.columnHandle = column;
        this.schema = schema;
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf) {
        if (columnHandle == null) {
            return Optional.empty();
        }
        Object value = schema.decode(byteBuf);
        Map<DecoderColumnHandle, FieldValueProvider> primitiveColumn = new HashMap<>();
        if (value == null) {
            primitiveColumn.put(columnHandle, FieldValueProviders.nullValueProvider());
        } else {
            Type type = columnHandle.getType();
            if (type instanceof BooleanType) {
                primitiveColumn.put(columnHandle, booleanValueProvider(Boolean.valueOf((Boolean) value)));
            } else if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType
                    || type instanceof BigintType) {
                primitiveColumn.put(columnHandle, longValueProvider(Long.parseLong(value.toString())));
            } else if (type instanceof DoubleType) {
                primitiveColumn.put(columnHandle, doubleValueProvider(Double.parseDouble(value.toString())));
            } else if (type instanceof RealType) {
                primitiveColumn.put(columnHandle, longValueProvider(
                        Float.floatToIntBits((Float.parseFloat(value.toString())))));
            } else if (type instanceof VarbinaryType) {
                primitiveColumn.put(columnHandle, bytesValueProvider((byte[]) value));
            } else if (type instanceof VarcharType) {
                primitiveColumn.put(columnHandle, bytesValueProvider(value.toString().getBytes()));
            } else if (type instanceof DateType) {
                primitiveColumn.put(columnHandle, longValueProvider(((Date) value).getTime()));
            } else if (type instanceof TimeType) {
                primitiveColumn.put(columnHandle, longValueProvider(((Time) value).getTime()));
            } else if (type instanceof TimestampType) {
                primitiveColumn.put(columnHandle, longValueProvider(((Timestamp) value).getTime()));
            } else {
                primitiveColumn.put(columnHandle, bytesValueProvider(value.toString().getBytes()));
            }
        }
        return Optional.of(primitiveColumn);
    }
}

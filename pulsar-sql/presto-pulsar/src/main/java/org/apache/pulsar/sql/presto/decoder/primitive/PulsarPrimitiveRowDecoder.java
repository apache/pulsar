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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;

import io.prestosql.spi.type.*;
import org.apache.pulsar.sql.presto.PulsarRowDecoder;

import static io.prestosql.decoder.FieldValueProviders.*;
import static org.apache.pulsar.sql.presto.PulsarFieldValueProviders.doubleValueProvider;

/**
 * Primitive Schema PulsarRowDecoder.
 */
public class PulsarPrimitiveRowDecoder implements PulsarRowDecoder {

    private final DecoderColumnHandle columnHandle;

    public PulsarPrimitiveRowDecoder(DecoderColumnHandle column) {
        this.columnHandle = column;
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf) {
        byte[] data = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(data);
        Map<DecoderColumnHandle, FieldValueProvider> primitiveColumn = new HashMap<DecoderColumnHandle, FieldValueProvider>();
        Type type = columnHandle.getType();
        if (type instanceof BooleanType) {
            primitiveColumn.put(columnHandle, booleanValueProvider(Boolean.valueOf(new String(data))));
        } else if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType
                || type instanceof BigintType) {
            primitiveColumn.put(columnHandle, longValueProvider(Long.valueOf(new String(data))));
        } else if (type instanceof DoubleType) {
            primitiveColumn.put(columnHandle, doubleValueProvider(Double.valueOf(new String(data))));
        } else if (type instanceof RealType) {
            primitiveColumn.put(columnHandle, longValueProvider(Float.floatToIntBits((Float.valueOf(new String(data))))));
        } else if (type instanceof VarbinaryType || type instanceof VarcharType) {
            primitiveColumn.put(columnHandle, bytesValueProvider(data));
        } else if (type instanceof DateType || type instanceof TimeType || type instanceof TimestampType) {
            primitiveColumn.put(columnHandle, longValueProvider(Long.valueOf(new String(data))));
        } else {
            primitiveColumn.put(columnHandle, bytesValueProvider(data));
        }
        return Optional.of(primitiveColumn);
    }
}

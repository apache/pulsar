/*
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Copy from {@link io.trino.decoder.avro.AvroColumnDecoder} (presto-record-decoder-345)
 * with A little bit pulsar's extensions.
 * 1) support {@link io.trino.spi.type.TimestampType},{@link io.trino.spi.type.DateType}DATE,
 *  * {@link io.trino.spi.type.TimeType}.
 * 2) support {@link io.trino.spi.type.RealType}.
 */
public class PulsarAvroColumnDecoder {
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BooleanType.BOOLEAN,
            TinyintType.TINYINT,
            SmallintType.SMALLINT,
            IntegerType.INTEGER,
            BigintType.BIGINT,
            RealType.REAL,
            DoubleType.DOUBLE,
            TimestampType.TIMESTAMP_MILLIS,
            DateType.DATE,
            TimeType.TIME_MILLIS,
            VarbinaryType.VARBINARY);

    private final Type columnType;
    private final String columnMapping;
    private final String columnName;

    public PulsarAvroColumnDecoder(DecoderColumnHandle columnHandle) {
        try {
            requireNonNull(columnHandle, "columnHandle is null");
            this.columnType = columnHandle.getType();
            this.columnMapping = columnHandle.getMapping();
            this.columnName = columnHandle.getName();
            checkArgument(!columnHandle.isInternal(),
                    "unexpected internal column '%s'", columnName);
            checkArgument(columnHandle.getFormatHint() == null,
                    "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnName);
            checkArgument(columnHandle.getDataFormat() == null,
                    "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnName);
            checkArgument(columnHandle.getMapping() != null,
                    "mapping not defined for column '%s'", columnName);
            checkArgument(isSupportedType(columnType),
                    "Unsupported column type '%s' for column '%s'", columnType, columnName);
        } catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_USER_ERROR, e);
        }
    }

    private boolean isSupportedType(Type type) {
        if (isSupportedPrimitive(type)) {
            return true;
        }

        if (type instanceof ArrayType) {
            checkArgument(type.getTypeParameters().size() == 1,
                    "expecting exactly one type parameter for array");
            return isSupportedType(type.getTypeParameters().get(0));
        }

        if (type instanceof MapType) {
            List<Type> typeParameters = type.getTypeParameters();
            checkArgument(typeParameters.size() == 2,
                    "expecting exactly two type parameters for map");
            checkArgument(typeParameters.get(0) instanceof VarcharType,
                    "Unsupported column type '%s' for map key", typeParameters.get(0));
            return isSupportedType(type.getTypeParameters().get(1));
        }

        if (type instanceof RowType) {
            for (Type fieldType : type.getTypeParameters()) {
                if (!isSupportedType(fieldType)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private boolean isSupportedPrimitive(Type type) {
        return type instanceof VarcharType || type instanceof DecimalType || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    public FieldValueProvider decodeField(GenericRecord avroRecord) {
        Object avroColumnValue = locateNode(avroRecord, columnMapping);
        return new ObjectValueProvider(avroColumnValue, columnType, columnName);
    }

    private static Object locateNode(GenericRecord element, String columnMapping) {
        Object value = element;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(columnMapping)) {
            if (value == null) {
                return null;
            }
            value = ((GenericRecord) value).get(pathElement);
        }
        return value;
    }

    private static class ObjectValueProvider
            extends FieldValueProvider {
        private final Object value;
        private final Type columnType;
        private final String columnName;

        public ObjectValueProvider(Object value, Type columnType, String columnName) {
            this.value = value;
            this.columnType = columnType;
            this.columnName = columnName;
        }

        @Override
        public boolean isNull() {
            return value == null;
        }

        @Override
        public double getDouble() {
            if (value instanceof Double || value instanceof Float) {
                return ((Number) value).doubleValue();
            }
            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                    format("cannot decode object of '%s' as '%s' for column '%s'",
                            value.getClass(), columnType, columnName));
        }

        @Override
        public boolean getBoolean() {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                    format("cannot decode object of '%s' as '%s' for column '%s'",
                            value.getClass(), columnType, columnName));
        }

        @Override
        public long getLong() {
            if (value instanceof Long || value instanceof Integer) {
                final long payload = ((Number) value).longValue();
                if (TimestampType.TIMESTAMP_MILLIS.equals(columnType)) {
                    return payload * Timestamps.MICROSECONDS_PER_MILLISECOND;
                }
                if (TimeType.TIME_MILLIS.equals(columnType)) {
                    return payload * Timestamps.PICOSECONDS_PER_MILLISECOND;
                }
                return payload;
            }

            if (columnType instanceof RealType) {
                return floatToIntBits((Float) value);
            }

            if (columnType instanceof DecimalType) {
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return new BigInteger(bytes).longValue();
            }

            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                    format("cannot decode object of '%s' as '%s' for column '%s'",
                            value.getClass(), columnType, columnName));
        }

        @Override
        public Slice getSlice() {
            return PulsarAvroColumnDecoder.getSlice(value, columnType, columnName);
        }

        @Override
        public Block getBlock() {
            return serializeObject(null, value, columnType, columnName);
        }
    }

    private static Slice getSlice(Object value, Type type, String columnName) {
        if (type instanceof VarcharType && (value instanceof CharSequence || value instanceof GenericEnumSymbol)) {
            return truncateToLength(utf8Slice(value.toString()), type);
        }

        if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                return Slices.wrappedBuffer((ByteBuffer) value);
            } else if (value instanceof GenericFixed) {
                return Slices.wrappedBuffer(((GenericFixed) value).bytes());
            }
        }

        // The returned Slice size must be equals to 18 Byte
        if (type instanceof DecimalType) {
            ByteBuffer buffer = (ByteBuffer) value;
            BigInteger bigInteger = new BigInteger(buffer.array());
            return Decimals.encodeUnscaledValue(bigInteger);
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                format("cannot decode object of '%s' as '%s' for column '%s'",
                        value.getClass(), type, columnName));
    }

    private static Block serializeObject(BlockBuilder builder, Object value, Type type, String columnName) {
        if (type instanceof ArrayType) {
            return serializeList(builder, value, type, columnName);
        }
        if (type instanceof MapType) {
            return serializeMap(builder, value, type, columnName);
        }
        if (type instanceof RowType) {
            return serializeRow(builder, value, type, columnName);
        }
        serializePrimitive(builder, value, type, columnName);
        return null;
    }

    private static Block serializeList(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName) {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }
        List<?> list = (List<?>) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type elementType = typeParameters.get(0);

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, list.size());
        for (Object element : list) {
            serializeObject(blockBuilder, element, elementType, columnName);
        }
        if (parentBlockBuilder != null) {
            type.writeObject(parentBlockBuilder, blockBuilder.build());
            return null;
        }
        return blockBuilder.build();
    }

    private static void serializePrimitive(BlockBuilder blockBuilder, Object value, Type type, String columnName) {
        requireNonNull(blockBuilder, "parent blockBuilder is null");

        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        if (type instanceof BooleanType) {
            type.writeBoolean(blockBuilder, (Boolean) value);
            return;
        }

        if (value instanceof Integer || value instanceof Long) {
            final long payload = ((Number) value).longValue();
            if (type instanceof BigintType || type instanceof IntegerType
                    || type instanceof SmallintType || type instanceof TinyintType) {
                type.writeLong(blockBuilder, payload);
                return;
            }
            if (TimestampType.TIMESTAMP_MILLIS.equals(type)) {
                type.writeLong(blockBuilder, payload * Timestamps.MICROSECONDS_PER_MILLISECOND);
                return;
            }
            if (TimeType.TIME_MILLIS.equals(type)) {
                type.writeLong(blockBuilder, payload * Timestamps.PICOSECONDS_PER_MILLISECOND);
                return;
            }
        }

        if (type instanceof DoubleType) {
            type.writeDouble(blockBuilder, (Double) value);
            return;
        }

        if (type instanceof RealType) {
            type.writeLong(blockBuilder, floatToIntBits((Float) value));
            return;
        }

        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            type.writeSlice(blockBuilder, getSlice(value, type, columnName));
            return;
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                format("cannot decode object of '%s' as '%s' for column '%s'",
                        value.getClass(), type, columnName));
    }

    private static Block serializeMap(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName) {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }

        Map<?, ?> map = (Map<?, ?>) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);

        BlockBuilder blockBuilder;
        if (parentBlockBuilder != null) {
            blockBuilder = parentBlockBuilder;
        } else {
            blockBuilder = type.createBlockBuilder(null, 1);
        }

        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() != null) {
                keyType.writeSlice(entryBuilder, truncateToLength(utf8Slice(entry.getKey().toString()), keyType));
                serializeObject(entryBuilder, entry.getValue(), valueType, columnName);
            }
        }
        blockBuilder.closeEntry();

        if (parentBlockBuilder == null) {
            return blockBuilder.getObject(0, Block.class);
        }
        return null;
    }

    private static Block serializeRow(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName) {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parent block builder is null");
            parentBlockBuilder.appendNull();
            return null;
        }

        BlockBuilder blockBuilder;
        if (parentBlockBuilder != null) {
            blockBuilder = parentBlockBuilder;
        } else {
            blockBuilder = type.createBlockBuilder(null, 1);
        }
        BlockBuilder singleRowBuilder = blockBuilder.beginBlockEntry();
        GenericRecord record = (GenericRecord) value;
        List<Field> fields = ((RowType) type).getFields();
        for (Field field : fields) {
            checkState(field.getName().isPresent(), "field name not found");
            serializeObject(singleRowBuilder, record.get(field.getName().get()), field.getType(), columnName);
        }
        blockBuilder.closeEntry();
        if (parentBlockBuilder == null) {
            return blockBuilder.getObject(0, Block.class);
        }
        return null;
    }
}

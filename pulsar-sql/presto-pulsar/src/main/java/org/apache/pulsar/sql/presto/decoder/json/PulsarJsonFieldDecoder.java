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
package org.apache.pulsar.sql.presto.decoder.json;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.json.JsonFieldDecoder;
import io.trino.decoder.json.JsonRowDecoderFactory;
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
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Copy from {@link io.trino.decoder.json.DefaultJsonFieldDecoder} (presto-record-decoder-345)
 * with some pulsar's extensions.
 * 1) support {@link io.trino.spi.type.ArrayType}.
 * 2) support {@link io.trino.spi.type.MapType}.
 * 3) support {@link io.trino.spi.type.RowType}.
 * 4) support {@link io.trino.spi.type.TimestampType},{@link io.trino.spi.type.DateType},
 * {@link io.trino.spi.type.TimeType}.
 * 5) support {@link io.trino.spi.type.RealType}.
 */
public class PulsarJsonFieldDecoder
        implements JsonFieldDecoder {

    private final DecoderColumnHandle columnHandle;
    private final long minValue;
    private final long maxValue;

    public PulsarJsonFieldDecoder(DecoderColumnHandle columnHandle) {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!isSupportedType(columnHandle.getType())) {
            JsonRowDecoderFactory.throwUnsupportedColumnType(columnHandle);
        }
        Pair<Long, Long> range = getNumRangeByType(columnHandle.getType());
        minValue = range.getKey();
        maxValue = range.getValue();

    }

    private static Pair<Long, Long> getNumRangeByType(Type type) {
        if (type == TinyintType.TINYINT) {
            return Pair.of((long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE);
        } else if (type == SmallintType.SMALLINT) {
            return Pair.of((long) Short.MIN_VALUE, (long) Short.MAX_VALUE);
        } else if (type == IntegerType.INTEGER) {
            return Pair.of((long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE);
        } else if (type == BigintType.BIGINT) {
            return Pair.of(Long.MIN_VALUE, Long.MAX_VALUE);
        } else {
            // those values will not be used if column type is not one of mentioned above
            return Pair.of(Long.MIN_VALUE, Long.MAX_VALUE);
        }
    }

    private boolean isSupportedType(Type type) {
        if (type instanceof DecimalType) {
            return true;
        }
        if (type instanceof VarcharType) {
            return true;
        }
        if (ImmutableList.of(
                BigintType.BIGINT,
                IntegerType.INTEGER,
                SmallintType.SMALLINT,
                TinyintType.TINYINT,
                BooleanType.BOOLEAN,
                DoubleType.DOUBLE,
                TimestampType.TIMESTAMP_MILLIS,
                DateType.DATE,
                TimeType.TIME_MILLIS,
                RealType.REAL
        ).contains(type)) {
            return true;
        }

        if (type instanceof ArrayType) {
            checkArgument(type.getTypeParameters().size() == 1, "expecting exactly one type parameter for array");
            return isSupportedType(type.getTypeParameters().get(0));
        }
        if (type instanceof MapType) {
            List<Type> typeParameters = type.getTypeParameters();
            checkArgument(typeParameters.size() == 2, "expecting exactly two type parameters for map");
            return isSupportedType(type.getTypeParameters().get(0)) && isSupportedType(type.getTypeParameters().get(1));
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

    @Override
    public FieldValueProvider decode(JsonNode value) {
        return new JsonValueProvider(value, columnHandle, minValue, maxValue);
    }

    /**
     * JsonValueProvider.
     */
    public static class JsonValueProvider
            extends FieldValueProvider {
        private final JsonNode value;
        private final DecoderColumnHandle columnHandle;
        private final long minValue;
        private final long maxValue;

        public JsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle, long minValue, long maxValue) {
            this.value = value;
            this.columnHandle = columnHandle;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        @Override
        public final boolean isNull() {
            return value.isMissingNode() || value.isNull();
        }

        @Override
        public boolean getBoolean() {
            return getBoolean(value, columnHandle.getType(), columnHandle.getName());
        }

        @Override
        public long getLong() {
            return getLong(value, columnHandle.getType(), columnHandle.getName(), minValue, maxValue);
        }

        @Override
        public double getDouble() {
            return getDouble(value, columnHandle.getType(), columnHandle.getName());
        }

        @Override
        public Slice getSlice() {
            return getSlice(value, columnHandle.getType(), columnHandle.getName());
        }

        @Override
        public Block getBlock() {
            return serializeObject(null, value, columnHandle.getType(), columnHandle.getName());
        }


        public static boolean getBoolean(JsonNode value, Type type, String columnName) {
            if (value.isValueNode()) {
                return value.asBoolean();
            }
            throw new TrinoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse non-value node as '%s' for column '%s'", type, columnName));
        }

        public static long getLong(JsonNode value, Type type, String columnName, long minValue, long maxValue) {
            try {
                if (type instanceof RealType) {
                    return floatToIntBits(Float.parseFloat(value.asText()));
                }

                // If it is decimalType, need to eliminate the decimal point,
                // and give it to presto to set the decimal point
                if (type instanceof DecimalType) {
                    String decimalLong = value.asText().replace(".", "");
                    return Long.parseLong(decimalLong);
                }

                Long longValue;
                if (value.isIntegralNumber() && !value.isBigInteger()) {
                    longValue = value.longValue();
                } else if (value.isValueNode()) {
                    longValue = Long.parseLong(value.asText());
                } else {
                    longValue = null;
                }

                if (longValue != null && longValue >= minValue && longValue <= maxValue) {
                    if (TimestampType.TIMESTAMP_MILLIS.equals(type)) {
                        return longValue * Timestamps.MICROSECONDS_PER_MILLISECOND;
                    }
                    if (TimeType.TIME_MILLIS.equals(type)) {
                        return longValue * Timestamps.PICOSECONDS_PER_MILLISECOND;
                    }
                    return longValue;
                }
            } catch (NumberFormatException ignore) {
                // ignore
            }
            throw new TrinoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse value '%s' as '%s' for column '%s'", value.asText(), type, columnName));
        }

        public static double getDouble(JsonNode value, Type type, String columnName) {
            try {
                if (value.isNumber()) {
                    return value.doubleValue();
                }
                if (value.isValueNode()) {
                    return parseDouble(value.asText());
                }
            } catch (NumberFormatException ignore) {
                // ignore
            }
            throw new TrinoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse value '%s' as '%s' for column '%s'", value.asText(), type, columnName));

        }

        private static Slice getSlice(JsonNode value, Type type, String columnName) {
            String textValue = value.isValueNode() ? value.asText() : value.toString();

            // If it is decimalType, need to eliminate the decimal point,
            // and give it to presto to set the decimal point
            if (type instanceof DecimalType) {
                textValue = textValue.replace(".", "");
                BigInteger bigInteger = new BigInteger(textValue);
                return Decimals.encodeUnscaledValue(bigInteger);
            }

            Slice slice = utf8Slice(textValue);
            if (type instanceof VarcharType) {
                slice = truncateToLength(slice, type);
            }
            return slice;
        }

        private Block serializeObject(BlockBuilder builder, Object value, Type type, String columnName) {
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

        private Block serializeList(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName) {
            if (value == null) {
                checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
                parentBlockBuilder.appendNull();
                return null;
            }

            checkState(value instanceof ArrayNode, "Json array node must is ArrayNode type");

            Iterator<JsonNode> jsonNodeIterator = ((ArrayNode) value).elements();

            List<Type> typeParameters = type.getTypeParameters();
            Type elementType = typeParameters.get(0);

            BlockBuilder blockBuilder = elementType.createBlockBuilder(null, ((ArrayNode) value).size());

            while (jsonNodeIterator.hasNext()) {
                Object element = jsonNodeIterator.next();
                serializeObject(blockBuilder, element, elementType, columnName);
            }

            if (parentBlockBuilder != null) {
                type.writeObject(parentBlockBuilder, blockBuilder.build());
                return null;
            }
            return blockBuilder.build();
        }

        private void serializePrimitive(BlockBuilder blockBuilder, Object node, Type type, String columnName) {
            requireNonNull(blockBuilder, "parent blockBuilder is null");

            JsonNode value;
            if (node == null) {
                blockBuilder.appendNull();
                return;
            }

            if (node instanceof JsonNode) {
                value = (JsonNode) node;
            } else {
                throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                        format("primitive object of '%s' as '%s' for column '%s' cann't convert to JsonNode",
                                node.getClass(), type, columnName));
            }

            if (type instanceof BooleanType) {
                type.writeBoolean(blockBuilder, getBoolean(value, type, columnName));
                return;
            }

            if (type instanceof RealType || type instanceof BigintType
                    || type instanceof IntegerType || type instanceof SmallintType
                    || type instanceof TinyintType || type instanceof TimestampType
                    || type instanceof TimeType || type instanceof DateType) {
                Pair<Long, Long> numRange = getNumRangeByType(type);
                type.writeLong(blockBuilder, getLong(value, type, columnName, numRange.getKey(), numRange.getValue()));
                return;
            }

            if (type instanceof DoubleType) {
                type.writeDouble(blockBuilder, getDouble(value, type, columnName));
                return;
            }

            if (type instanceof VarcharType || type instanceof VarbinaryType) {
                type.writeSlice(blockBuilder, getSlice(value, type, columnName));
                return;
            }

            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                    format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
        }

        private Block serializeMap(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName) {
            if (value == null) {
                checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
                parentBlockBuilder.appendNull();
                return null;
            }
            checkState(value instanceof ObjectNode, "Json map node must  is ObjectNode type");

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

            Iterator<Map.Entry<String, JsonNode>> fields = ((ObjectNode) value).fields();
            while (fields.hasNext()) {
                Map.Entry entry = fields.next();
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


        private Block serializeRow(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName) {
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

            List<RowType.Field> fields = ((RowType) type).getFields();

            checkState(value instanceof ObjectNode, "Json row node must be ObjectNode type");

            for (RowType.Field field : fields) {
                checkState(field.getName().isPresent(), "field name not found");
                serializeObject(singleRowBuilder, ((ObjectNode) value).get(field.getName().get()),
                        field.getType(), columnName);
            }
            blockBuilder.closeEntry();
            if (parentBlockBuilder == null) {
                return blockBuilder.getObject(0, Block.class);
            }
            return null;
        }

    }

    private static final Logger log = Logger.get(PulsarJsonFieldDecoder.class);

}

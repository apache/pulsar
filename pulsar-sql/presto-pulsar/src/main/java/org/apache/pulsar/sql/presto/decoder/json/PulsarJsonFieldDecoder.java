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
package org.apache.pulsar.sql.presto.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.json.JsonFieldDecoder;
import io.prestosql.decoder.json.JsonRowDecoderFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * copy from {@link io.prestosql.decoder.json.DefaultJsonFieldDecoder} (presto-record-decoder-345) with some pulsar's extension :
 * 1) support array
 * 2) support map
 * 3) support row
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

        if (columnHandle.getType() == TINYINT) {
            minValue = Byte.MIN_VALUE;
            maxValue = Byte.MAX_VALUE;
        } else if (columnHandle.getType() == SMALLINT) {
            minValue = Short.MIN_VALUE;
            maxValue = Short.MAX_VALUE;
        } else if (columnHandle.getType() == INTEGER) {
            minValue = Integer.MIN_VALUE;
            maxValue = Integer.MAX_VALUE;
        } else if (columnHandle.getType() == BIGINT) {
            minValue = Long.MIN_VALUE;
            maxValue = Long.MAX_VALUE;
        } else {
            // those values will not be used if column type is not one of mentioned above
            minValue = Long.MAX_VALUE;
            maxValue = Long.MIN_VALUE;
        }
    }

    private boolean isSupportedType(Type type) {
        if (isVarcharType(type)) {
            return true;
        }
        if (ImmutableList.of(
                BIGINT,
                INTEGER,
                SMALLINT,
                TINYINT,
                BOOLEAN,
                DOUBLE
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
            if (value.isValueNode()) {
                return value.asBoolean();
            }
            throw new PrestoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
        }

        @Override
        public long getLong() {
            try {
                long longValue;
                if (value.isIntegralNumber() && !value.isBigInteger()) {
                    longValue = value.longValue();
                    if (longValue >= minValue && longValue <= maxValue) {
                        return longValue;
                    }
                } else if (value.isValueNode()) {
                    longValue = parseLong(value.asText());
                    if (longValue >= minValue && longValue <= maxValue) {
                        return longValue;
                    }
                }
            } catch (NumberFormatException ignore) {
                // ignore
            }
            throw new PrestoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
        }

        @Override
        public double getDouble() {
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
            throw new PrestoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
        }

        @Override
        public Slice getSlice() {
            String textValue = value.isValueNode() ? value.asText() : value.toString();
            Slice slice = utf8Slice(textValue);
            if (isVarcharType(columnHandle.getType())) {
                slice = truncateToLength(slice, columnHandle.getType());
            }
            return slice;
        }

        @Override
        public Block getBlock() {
            return serializeObject(null, value, columnHandle.getType(), columnHandle.getName());
        }


        private static Slice getSlice(Object value, Type type, String columnName) {
            String textValue;
            if (type instanceof JsonNode) {
                JsonNode jsonNode = (JsonNode) value;
                textValue = jsonNode.isValueNode() ? jsonNode.asText() : jsonNode.toString();
            } else {
                textValue = value.toString();
            }

            Slice slice = utf8Slice(textValue);
            if (isVarcharType(type)) {
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

            com.fasterxml.jackson.databind.node.ArrayNode arrayNode = (com.fasterxml.jackson.databind.node.ArrayNode) value;

            Iterator<JsonNode> jsonNodeIterator = arrayNode.elements();

            //  List<?> list = (List<?>) value;
            List<Type> typeParameters = type.getTypeParameters();
            Type elementType = typeParameters.get(0);

            BlockBuilder blockBuilder = elementType.createBlockBuilder(null, arrayNode.size());

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

        private void serializePrimitive(BlockBuilder blockBuilder, Object value, Type type, String columnName) {
            requireNonNull(blockBuilder, "parent blockBuilder is null");

            if (value == null) {
                blockBuilder.appendNull();
                return;
            }

            if (type instanceof BooleanType) {
                type.writeBoolean(blockBuilder, (Boolean) value);
                return;
            }

            if ((value instanceof Integer || value instanceof Long) && (type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
                return;
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

            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
        }

        private Block serializeMap(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName) {
            if (value == null) {
                checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
                parentBlockBuilder.appendNull();
                return null;
            }

            com.fasterxml.jackson.databind.node.ObjectNode objectNode = (com.fasterxml.jackson.databind.node.ObjectNode) value;

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

            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
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
            // GenericRecord record = (GenericRecord) value;
            com.fasterxml.jackson.databind.node.ObjectNode objectNode = (com.fasterxml.jackson.databind.node.ObjectNode) value;

            List<RowType.Field> fields = ((RowType) type).getFields();

            for (RowType.Field field : fields) {
                checkState(field.getName().isPresent(), "field name not found");
                serializeObject(singleRowBuilder, objectNode.get(field.getName().get()), field.getType(), columnName);
            }
            blockBuilder.closeEntry();
            if (parentBlockBuilder == null) {
                return blockBuilder.getObject(0, Block.class);
            }
            return null;
        }

    }
}

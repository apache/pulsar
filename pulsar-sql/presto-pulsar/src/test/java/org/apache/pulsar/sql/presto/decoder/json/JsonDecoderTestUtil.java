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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.*;
import org.apache.pulsar.sql.presto.decoder.DecoderTestUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.*;

/**
 *
 * TestUtil for JsonDecoder
 */
public class JsonDecoderTestUtil extends DecoderTestUtil {

    public JsonDecoderTestUtil() {
        super();
    }

    @Override
    public void checkPrimitiveValue(Object actual, Object expected) {
        assertTrue(expected instanceof JsonNode);
        if (actual == null || null == expected) {
            assertNull(expected);
            assertNull(actual);
        } else if (actual instanceof CharSequence) {
            assertEquals(actual.toString(), ((JsonNode) expected).asText());
        } else if (actual instanceof SqlVarbinary) {
            try {
                assertEquals(((SqlVarbinary) actual).getBytes(), ((JsonNode) expected).binaryValue());
            } catch (IOException e) {
                fail(format("JsonNode %s formate binary Value failed", ((JsonNode) expected).getNodeType().name()));
            }
        } else if (isIntegralType(actual)) {
            assertEquals(((Number) actual).longValue(), ((JsonNode) expected).asLong());
        } else if (isRealType(actual)) {
            assertEquals(((Number) actual).doubleValue(), ((JsonNode) expected).asDouble());
        } else {
            assertEquals(actual, expected);
        }
    }

    @Override
    public void checkMapValues(Block block, Type type, Object value) {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof MapType, "Unexpected type");
        assertTrue(((MapType) type).getKeyType() instanceof VarcharType, "Unexpected key type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        assertTrue(value instanceof ObjectNode, "map node isn't ObjectNode type");

        ObjectNode expected = (ObjectNode) value;

        Iterator<Map.Entry<String, JsonNode>> fields = expected.fields();

        assertEquals(block.getPositionCount(), Iterators.size(expected.fields()) * 2);
        Type valueType = ((MapType) type).getValueType();
        if (valueType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(Iterators.any(fields, entry -> entry.getKey().equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block arrayBlock = block.getObject(index + 1, Block.class);
                checkArrayValues(arrayBlock, valueType, expected.get(actualKey));
            }
        } else if (valueType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(Iterators.any(fields, entry -> entry.getKey().equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block mapBlock = block.getObject(index + 1, Block.class);
                checkMapValues(mapBlock, valueType, expected.get(actualKey));
            }
        } else if (valueType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(Iterators.any(fields, entry -> entry.getKey().equals(actualKey)));

                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block rowBlock = block.getObject(index + 1, Block.class);
                checkRowValues(rowBlock, valueType, expected.get(actualKey));
            }
        } else {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                Map.Entry<String, JsonNode> entry = Iterators.tryFind(fields, e -> e.getKey().equals(actualKey)).get();
                assertNotNull(entry);
                assertNotNull(entry.getKey());
                checkPrimitiveValue(getObjectValue(valueType, block, index + 1), entry.getValue());
            }
        }
    }

    @Override
    public void checkRowValues(Block block, Type type, Object value) {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof RowType, "Unexpected type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        ObjectNode record = (ObjectNode) value;
        RowType rowType = (RowType) type;
        assertEquals(Iterators.size(record.fields()), rowType.getFields().size(), "Json field size mismatch");
        assertEquals(block.getPositionCount(), rowType.getFields().size(), "Presto type field size mismatch");
        for (int fieldIndex = 0; fieldIndex < rowType.getFields().size(); fieldIndex++) {
            RowType.Field rowField = rowType.getFields().get(fieldIndex);
            Object expectedValue = record.get(rowField.getName().get());
            if (block.isNull(fieldIndex)) {
                assertNull(expectedValue);
                continue;
            }
            checkField(block, rowField.getType(), fieldIndex, expectedValue);
        }
    }

    @Override
    public void checkArrayValues(Block block, Type type, Object value) {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof ArrayType, "Unexpected type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        assertTrue(value instanceof ArrayNode, "Array node isn't ArrayNode type");
        ArrayNode arrayNode = (ArrayNode) value;

        assertEquals(block.getPositionCount(), arrayNode.size());
        Type elementType = ((ArrayType) type).getElementType();
        if (elementType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(arrayNode.get(index));
                    continue;
                }
                Block arrayBlock = block.getObject(index, Block.class);
                checkArrayValues(arrayBlock, elementType, arrayNode.get(index));
            }
        } else if (elementType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(arrayNode.get(index));
                    continue;
                }
                Block mapBlock = block.getObject(index, Block.class);
                checkMapValues(mapBlock, elementType, arrayNode.get(index));
            }
        } else if (elementType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(arrayNode.get(index));
                    continue;
                }
                Block rowBlock = block.getObject(index, Block.class);
                checkRowValues(rowBlock, elementType, arrayNode.get(index));
            }
        } else {
            for (int index = 0; index < block.getPositionCount(); index++) {
                checkPrimitiveValue(getObjectValue(elementType, block, index), arrayNode.get(index));
            }
        }
    }

}

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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.*;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.sql.presto.decoder.DecoderTestUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.*;

/**
 * TestUtil for AvroDecoder
 */
public class AvroDecoderTestUtil extends DecoderTestUtil {
    public AvroDecoderTestUtil() {
        super();
    }

    public void checkPrimitiveValue(Object actual, Object expected) {
        if (actual == null || expected == null) {
            assertNull(expected);
            assertNull(actual);
        } else if (actual instanceof CharSequence) {
            assertTrue(expected instanceof CharSequence || expected instanceof GenericEnumSymbol);
            assertEquals(actual.toString(), expected.toString());
        } else if (actual instanceof SqlVarbinary) {
            if (expected instanceof GenericFixed) {
                assertEquals(((SqlVarbinary) actual).getBytes(), ((GenericFixed) expected).bytes());
            } else if (expected instanceof ByteBuffer) {
                assertEquals(((SqlVarbinary) actual).getBytes(), ((ByteBuffer) expected).array());
            } else {
                fail(format("Unexpected value type %s", actual.getClass()));
            }
        } else if (isIntegralType(actual) && isIntegralType(expected)) {
            assertEquals(((Number) actual).longValue(), ((Number) expected).longValue());
        } else if (isRealType(actual) && isRealType(expected)) {
            assertEquals(((Number) actual).doubleValue(), ((Number) expected).doubleValue());
        } else {
            assertEquals(actual, expected);
        }
    }


    public void checkArrayValues(Block block, Type type, Object value) {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof ArrayType, "Unexpected type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        List<?> list = (List<?>) value;

        assertEquals(block.getPositionCount(), list.size());
        Type elementType = ((ArrayType) type).getElementType();
        if (elementType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block arrayBlock = block.getObject(index, Block.class);
                checkArrayValues(arrayBlock, elementType, list.get(index));
            }
        } else if (elementType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block mapBlock = block.getObject(index, Block.class);
                checkMapValues(mapBlock, elementType, list.get(index));
            }
        } else if (elementType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index++) {
                if (block.isNull(index)) {
                    assertNull(list.get(index));
                    continue;
                }
                Block rowBlock = block.getObject(index, Block.class);
                checkRowValues(rowBlock, elementType, list.get(index));
            }
        } else {
            for (int index = 0; index < block.getPositionCount(); index++) {
                checkPrimitiveValue(getObjectValue(elementType, block, index), list.get(index));
            }
        }
    }

    /**
     * fix key as org.apache.avro.util.Utf8
     *
     * @param block
     * @param type
     * @param value
     */
    public void checkMapValues(Block block, Type type, Object value) {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof MapType, "Unexpected type");
        assertTrue(((MapType) type).getKeyType() instanceof VarcharType, "Unexpected key type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");


        Map<?, ?> expected = (Map<?, ?>) value;

        assertEquals(block.getPositionCount(), expected.size() * 2);
        Type valueType = ((MapType) type).getValueType();
        if (valueType instanceof ArrayType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.keySet().stream().anyMatch(e -> e.toString().equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block arrayBlock = block.getObject(index + 1, Block.class);
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().toString().equals(actualKey)).findFirst().get().getValue();
                checkArrayValues(arrayBlock, valueType, keyValue);
            }
        } else if (valueType instanceof MapType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.keySet().stream().anyMatch(e -> e.toString().equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block mapBlock = block.getObject(index + 1, Block.class);
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().toString().equals(actualKey)).findFirst().get().getValue();
                checkMapValues(mapBlock, valueType, keyValue);
            }
        } else if (valueType instanceof RowType) {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.keySet().stream().anyMatch(e -> e.toString().equals(actualKey)));
                if (block.isNull(index + 1)) {
                    assertNull(expected.get(actualKey));
                    continue;
                }
                Block rowBlock = block.getObject(index + 1, Block.class);
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().toString().equals(actualKey)).findFirst().get().getValue();
                checkRowValues(rowBlock, valueType, keyValue);
            }
        } else {
            for (int index = 0; index < block.getPositionCount(); index += 2) {
                String actualKey = VARCHAR.getSlice(block, index).toStringUtf8();
                assertTrue(expected.keySet().stream().anyMatch(e -> e.toString().equals(actualKey)));
                Object keyValue = expected.entrySet().stream().filter(e -> e.getKey().toString().equals(actualKey)).findFirst().get().getValue();
                checkPrimitiveValue(getObjectValue(valueType, block, index + 1), keyValue);
            }
        }
    }

    public void checkRowValues(Block block, Type type, Object value) {
        assertNotNull(type, "Type is null");
        assertTrue(type instanceof RowType, "Unexpected type");
        assertNotNull(block, "Block is null");
        assertNotNull(value, "Value is null");

        GenericRecord record = (GenericRecord) value;
        RowType rowType = (RowType) type;
        assertEquals(record.getSchema().getFields().size(), rowType.getFields().size(), "Avro field size mismatch");
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

}

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
package org.apache.pulsar.sql.presto.decoder;

import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.*;

/**
 * Abstract util superclass for  XXDecoderTestUtil (e.g. AvroDecoderTestUtil 、JsonDecoderTestUtil)
 */
public abstract class DecoderTestUtil {

    protected DecoderTestUtil() {

    }

    public abstract void checkArrayValues(Block block, Type type, Object value);

    public abstract void checkMapValues(Block block, Type type, Object value);

    public abstract void checkRowValues(Block block, Type type, Object value);

    public abstract void checkPrimitiveValue(Object actual, Object expected);

    public void checkField(Block actualBlock, Type type, int position, Object expectedValue) {
        assertNotNull(type, "Type is null");
        assertNotNull(actualBlock, "actualBlock is null");
        assertTrue(!actualBlock.isNull(position));
        assertNotNull(expectedValue, "expectedValue is null");

        if (type instanceof ArrayType) {
            checkArrayValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        } else if (type instanceof MapType) {
            checkMapValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        } else if (type instanceof RowType) {
            checkRowValues(actualBlock.getObject(position, Block.class), type, expectedValue);
        } else {
            checkPrimitiveValue(getObjectValue(type, actualBlock, position), expectedValue);
        }
    }

    public boolean isIntegralType(Object value) {
        return value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte;
    }

    public boolean isRealType(Object value) {
        return value instanceof Float || value instanceof Double;
    }

    public Object getObjectValue(Type type, Block block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        return type.getObjectValue(SESSION, block, position);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, Slice value) {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getSlice(), value);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, String value) {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getSlice().toStringUtf8(), value);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long value) {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getLong(), value);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, double value) {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getDouble(), value, 0.0001);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, boolean value) {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getBoolean(), value);
    }

    public void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, BigDecimal value) {
        FieldValueProvider provider = decodedRow.get(handle);
        DecimalType decimalType = (DecimalType) handle.getType();
        BigDecimal actualDecimal;
        if (decimalType.getFixedSize() == UNSCALED_DECIMAL_128_SLICE_LENGTH) {
            Slice slice = provider.getSlice();
            BigInteger bigInteger = Decimals.decodeUnscaledValue(slice);
            actualDecimal = new BigDecimal(bigInteger, decimalType.getScale());
        } else {
            actualDecimal = BigDecimal.valueOf(provider.getLong(), decimalType.getScale());
        }
        assertNotNull(provider);
        assertEquals(actualDecimal, value);
    }

    public void checkIsNull(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle) {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertTrue(provider.isNull());
    }
}

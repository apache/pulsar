/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.impl;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class AbstractStatefulHashTest {

    @Mocked
    private AbstractStatefulHash hash;

    @Test
    public void testUpdateByteArray() {
        final byte[] input = new byte[42];
        new Expectations(hash) {
            {
                hash.updateUnchecked(input, 0, input.length);
            }
        };
        hash.update(input);
    }

    @Test
    public void testUpdateByteArrayIntInt() {
        final byte[] input = new byte[42];
        new Expectations(hash) {
            {
                hash.updateUnchecked(input, 5, 10);
            }
        };
        hash.update(input, 5, 10);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUpdateByteArrayIntNegInt() {
        final byte[] input = new byte[42];
        new Expectations(hash) {
        };
        hash.update(input, 1, -1);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testUpdateByteArrayNegIntInt() {
        final byte[] input = new byte[42];
        new Expectations(hash) {
        };
        hash.update(input, -1, 10);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testUpdateByteArrayIntIntOverflow() {
        final byte[] input = new byte[42];
        new Expectations(hash) {
        };
        hash.update(input, 40, 3);
    }

    @Test
    public void testUpdateByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20);
        input.position(5);
        input.limit(15);
        new Expectations(hash) {
            {
                hash.updateUnchecked(input.array(), input.arrayOffset() + 5, 10);
            }
        };
        hash.update(input);
        assertEquals(input.limit(), input.position());
    }

    @Test
    public void testUpdateReadOnlyByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20).asReadOnlyBuffer();
        input.position(5);
        input.limit(15);
        new Expectations(hash) {
            {
                hash.updateUnchecked(withInstanceOf(byte[].class), 0, 10);
            }
        };
        hash.update(input);
        assertEquals(input.limit(), input.position());
    }

    @Test
    public void testGetBytes() {
        final List<byte[]> captures = new ArrayList<>();
        new Expectations(hash) {
            {
                hash.length();
                result = 5;
                hash.writeBytes(withCapture(captures), 0, 5);
            }
        };
        hash.getBytes();
        assertEquals(5, captures.get(0).length);
    }

    @Test
    public void testGetBytesByteArrayInt() {
        final byte[] output = new byte[5];
        new Expectations(hash) {
            {
                hash.length();
                result = output.length;
                hash.getLong();
                result = 0x1234567890L;
            }
        };
        hash.getBytes(output, 0, output.length);
        assertEquals(new byte[] { (byte) 0x90, 0x78, 0x56, 0x34, 0x12 }, output);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testGetBytesByteArrayNegInt() {
        final byte[] output = new byte[5];
        new NonStrictExpectations(hash) {
            {
                hash.length();
                result = output.length;
            }
        };
        hash.getBytes(output, -1, output.length);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testGetBytesByteArrayIntOverflow() {
        final byte[] output = new byte[5];
        new Expectations(hash) {
        };
        hash.getBytes(output, 0, output.length + 1);
    }

    @Test
    public void testGetBytesByteArrayIntPartial() {
        final byte[] output = new byte[5];
        new Expectations(hash) {
            {
                hash.length();
                result = output.length + 1;
                hash.writeBytes(output, 0, output.length);
            }
        };
        hash.getBytes(output, 0, output.length);
    }

    @Test
    public void testGetByte() {
        new Expectations(hash) {
            {
                hash.getInt();
                result = 0x12345678;
            }
        };
        assertEquals(0x78, hash.getByte());
    }

    @Test
    public void testGetShort() {
        new Expectations(hash) {
            {
                hash.getInt();
                result = 0x12345678;
            }
        };
        assertEquals(0x5678, hash.getShort());
    }
}

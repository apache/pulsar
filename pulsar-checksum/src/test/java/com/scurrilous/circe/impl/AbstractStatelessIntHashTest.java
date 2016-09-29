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

import static org.testng.Assert.*;

import java.nio.ByteBuffer;

import mockit.Expectations;
import mockit.Mocked;

import org.testng.annotations.Test;

import com.scurrilous.circe.impl.AbstractStatelessIntHash;

@SuppressWarnings("javadoc")
public class AbstractStatelessIntHashTest {

    @Mocked
    private AbstractStatelessIntHash hash;

    @Test
    public void testCalculateByteArray() {
        final byte[] input = new byte[10];
        new Expectations(hash) {
            {
                hash.calculateUnchecked(input, 0, input.length);
            }
        };
        hash.calculate(input);
    }

    @Test
    public void testCalculateByteArrayIntInt() {
        final byte[] input = new byte[10];
        new Expectations(hash) {
            {
                hash.calculateUnchecked(input, 2, 4);
            }
        };
        hash.calculate(input, 2, 4);
    }

    @Test
    public void testCalculateByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20);
        input.position(5);
        input.limit(15);
        new Expectations(hash) {
            {
                hash.calculateUnchecked(input.array(), input.arrayOffset() + 5, 10);
            }
        };
        hash.calculate(input);
        assertEquals(input.limit(), input.position());
    }

    @Test
    public void testCalculateReadOnlyByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20).asReadOnlyBuffer();
        input.position(5);
        input.limit(15);
        new Expectations(hash) {
            {
                hash.calculateUnchecked(withInstanceOf(byte[].class), 0, 10);
            }
        };
        hash.calculate(input);
        assertEquals(input.limit(), input.position());
    }
}

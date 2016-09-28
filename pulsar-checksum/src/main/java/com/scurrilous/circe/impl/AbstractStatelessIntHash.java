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

import java.nio.ByteBuffer;

import com.scurrilous.circe.StatelessIntHash;

/**
 * Base implementation for stateless (but not incremental) integer hash
 * functions.
 */
public abstract class AbstractStatelessIntHash implements StatelessIntHash {

    @Override
    public boolean supportsUnsafe() {
        return false;
    }

    @Override
    public int calculate(byte[] input) {
        return calculateUnchecked(input, 0, input.length);
    }

    @Override
    public int calculate(byte[] input, int index, int length) {
        if (length < 0)
            throw new IllegalArgumentException();
        if (index < 0 || index + length > input.length)
            throw new IndexOutOfBoundsException();
        return calculateUnchecked(input, index, length);
    }

    @Override
    public int calculate(ByteBuffer input) {
        final byte[] array;
        final int index;
        final int length = input.remaining();
        if (input.hasArray()) {
            array = input.array();
            index = input.arrayOffset() + input.position();
            input.position(input.limit());
        } else {
            array = new byte[length];
            index = 0;
            input.get(array);
        }
        return calculateUnchecked(array, index, length);
    }

    @Override
    public int calculate(long address, long length) {
        throw new UnsupportedOperationException();
    }

    /**
     * Evaluates this hash function for the given range of the given input
     * array. The index and length parameters have already been validated.
     * 
     * @param input the input array
     * @param index the starting index of the first input byte
     * @param length the length of the input range
     * @return the output of the hash function
     */
    protected abstract int calculateUnchecked(byte[] input, int index, int length);
}

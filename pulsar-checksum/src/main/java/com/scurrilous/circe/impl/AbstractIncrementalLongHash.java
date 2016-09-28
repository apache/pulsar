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

import com.scurrilous.circe.IncrementalLongHash;
import com.scurrilous.circe.StatefulLongHash;

/**
 * Base implementation for incremental stateless long integer hash functions.
 */
public abstract class AbstractIncrementalLongHash implements IncrementalLongHash {

    @Override
    public boolean supportsUnsafe() {
        return false;
    }

    @Override
    public StatefulLongHash createStateful() {
        return new IncrementalLongStatefulHash(this);
    }

    @Override
    public long calculate(byte[] input) {
        return resume(initial(), input);
    }

    @Override
    public long calculate(byte[] input, int index, int length) {
        return resume(initial(), input, index, length);
    }

    @Override
    public long calculate(ByteBuffer input) {
        return resume(initial(), input);
    }

    @Override
    public long calculate(long address, long length) {
        return resume(initial(), address, length);
    }

    @Override
    public long resume(long current, byte[] input) {
        return resumeUnchecked(current, input, 0, input.length);
    }

    @Override
    public long resume(long current, byte[] input, int index, int length) {
        if (length < 0)
            throw new IllegalArgumentException();
        if (index < 0 || index + length > input.length)
            throw new IndexOutOfBoundsException();
        return resumeUnchecked(current, input, index, length);
    }

    @Override
    public long resume(long current, ByteBuffer input) {
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
        return resumeUnchecked(current, array, index, length);
    }

    @Override
    public long resume(long current, long address, long length) {
        throw new UnsupportedOperationException();
    }

    /**
     * The initial state of the hash function, which is the same as the output
     * value for an empty input sequence.
     * 
     * @return the initial hash state/output
     */
    protected abstract long initial();

    /**
     * Evaluates this hash function as if the given range of the given input
     * array were appended to the previously hashed input. The index and length
     * parameters have already been validated.
     * 
     * @param current the hash output for input hashed so far
     * @param input the input array
     * @param index the starting index of the first input byte
     * @param length the length of the input range
     * @return the output of the hash function for the concatenated input
     */
    protected abstract long resumeUnchecked(long current, byte[] input, int index, int length);
}

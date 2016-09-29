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

import com.scurrilous.circe.StatefulHash;

/**
 * Base implementation for stateful hash functions.
 */
public abstract class AbstractStatefulHash implements StatefulHash {

    @Override
    public boolean supportsUnsafe() {
        return false;
    }

    @Override
    public void update(byte[] input) {
        updateUnchecked(input, 0, input.length);
    }

    @Override
    public void update(byte[] input, int index, int length) {
        if (length < 0)
            throw new IllegalArgumentException();
        if (index < 0 || index + length > input.length)
            throw new IndexOutOfBoundsException();
        updateUnchecked(input, index, length);
    }

    @Override
    public void update(ByteBuffer input) {
        final byte[] array;
        final int index;
        final int length = input.remaining();
        if (input.hasArray()) {
            array = input.array();
            index = input.arrayOffset() + input.position();
            input.position(input.limit());
        } else {
            // convert to unsafe access if possible
            if (input.isDirect() && supportsUnsafe()) {
                long address = DirectByteBufferAccessLoader.getAddress(input);
                if (address != 0) {
                    address += input.position();
                    input.position(input.limit());
                    update(address, length);
                    return;
                }
            }

            array = new byte[length];
            index = 0;
            input.get(array);
        }
        updateUnchecked(array, index, length);
    }

    @Override
    public void update(long address, long length) {
        throw new UnsupportedOperationException();
    }

    /**
     * Updates the state of this hash function with the given range of the given
     * input array. The index and length parameters have already been validated.
     * 
     * @param input the input array
     * @param index the starting index of the first input byte
     * @param length the length of the input range
     */
    protected abstract void updateUnchecked(byte[] input, int index, int length);

    @Override
    public byte[] getBytes() {
        final byte[] array = new byte[length()];
        writeBytes(array, 0, array.length);
        return array;
    }

    @Override
    public int getBytes(byte[] output, int index, int maxLength) {
        if (maxLength < 0)
            throw new IllegalArgumentException();
        if (index < 0 || index + maxLength > output.length)
            throw new IndexOutOfBoundsException();
        final int length = Math.min(maxLength, length());
        writeBytes(output, index, length);
        return length;
    }

    /**
     * Writes the output of this hash function into the given range of the given
     * byte array. The inputs have already been validated.
     * 
     * @param output the destination array for the output
     * @param index the starting index of the first output byte
     * @param length the number of bytes to write
     */
    protected void writeBytes(byte[] output, int index, int length) {
        long temp = getLong();
        for (int i = 0; i < length; ++i) {
            output[index + i] = (byte) temp;
            temp >>>= 8;
        }
    }

    @Override
    public byte getByte() {
        return (byte) getInt();
    }

    @Override
    public short getShort() {
        return (short) getInt();
    }
}

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
package com.scurrilous.circe;

import java.nio.ByteBuffer;

/**
 * Interface implemented by stateless hash functions with an output length
 * greater than 4 bytes and less than or equal to 8 bytes.
 */
public interface StatelessLongHash extends StatelessHash {

    /**
     * Returns a new instance of stateful version of this hash function.
     * 
     * @return the stateful version of this hash function
     */
    @Override
    StatefulLongHash createStateful();

    /**
     * Evaluates this hash function for the entire given input array.
     * 
     * @param input the input array
     * @return the output of the hash function
     */
    long calculate(byte[] input);

    /**
     * Evaluates this hash function for the given range of the given input
     * array.
     * 
     * @param input the input array
     * @param index the starting index of the first input byte
     * @param length the length of the input range
     * @return the output of the hash function
     * @throws IndexOutOfBoundsException if index is negative or
     *             {@code index + length} is greater than the array length
     */
    long calculate(byte[] input, int index, int length);

    /**
     * Evaluates this hash function with the remaining contents of the given
     * input buffer. This method leaves the buffer position at the limit.
     * 
     * @param input the input buffer
     * @return the output of the hash function
     */
    long calculate(ByteBuffer input);

    /**
     * Evaluates this hash function for the memory with the given address and
     * length. The arguments are generally not checked in any way and will
     * likely lead to a VM crash or undefined results if invalid.
     * 
     * @param address the base address of the input
     * @param length the length of the input
     * @return the output of the hash function
     * @throws UnsupportedOperationException if this function does not support
     *             unsafe memory access
     * @see #supportsUnsafe()
     */
    long calculate(long address, long length);
}

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
 * Incremental stateless long integer hash function, which has the property that
 * its output is the same as (or easily derivable from) its state. Specifically,
 * for any sequence M partitioned arbitrarily into two subsequences
 * M<sub>1</sub> M<sub>2</sub>:
 * 
 * <pre>
 * h(M) = h'(h(M<sub>1</sub>), M<sub>2</sub>)
 * </pre>
 * 
 * where h corresponds to the {@link StatelessLongHash#calculate calculate}
 * function and h' corresponds to the {@link #resume} function.
 * <p>
 * Note that stateful hash functions obtained from incremental stateless hash
 * functions are also {@linkplain StatefulHash#supportsIncremental incremental}.
 */
public interface IncrementalLongHash extends StatelessLongHash {

    /**
     * Evaluates this hash function as if the entire given input array were
     * appended to the previously hashed input.
     * 
     * @param current the hash output for input hashed so far
     * @param input the input array
     * @return the output of the hash function for the concatenated input
     */
    long resume(long current, byte[] input);

    /**
     * Evaluates this hash function as if the given range of the given input
     * array were appended to the previously hashed input.
     * 
     * @param current the hash output for input hashed so far
     * @param input the input array
     * @param index the starting index of the first input byte
     * @param length the length of the input range
     * @return the output of the hash function for the concatenated input
     * @throws IndexOutOfBoundsException if index is negative or
     *             {@code index + length} is greater than the array length
     */
    long resume(long current, byte[] input, int index, int length);

    /**
     * Evaluates this hash function as if the remaining contents of the given
     * input buffer were appended to the previously hashed input. This method
     * leaves the buffer position at the limit.
     * 
     * @param current the hash output for input hashed so far
     * @param input the input buffer
     * @return the output of the hash function for the concatenated input
     */
    long resume(long current, ByteBuffer input);

    /**
     * Evaluates this hash function as if the memory with the given address and
     * length were appended to the previously hashed input. The arguments are
     * generally not checked in any way and will likely lead to a VM crash or
     * undefined results if invalid.
     * 
     * @param current the hash output for input hashed so far
     * @param address the base address of the input
     * @param length the length of the input
     * @return the output of the hash function for the concatenated input
     * @throws UnsupportedOperationException if this function does not support
     *             unsafe memory access
     * @see #supportsUnsafe()
     */
    long resume(long current, long address, long length);
}

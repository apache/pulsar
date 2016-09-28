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
 * Represents a stateful hash function, which can accumulate input from multiple
 * method calls and provide output in various forms. Stateful hash functions
 * should not be used concurrently from multiple threads.
 * <p>
 * Stateful hash functions can be incremental or non-incremental. Incremental
 * hashing allows calling the {@link #update} methods to continue hashing using
 * accumulated state after calling any of the output methods (such as
 * {@link #getBytes}). Non-incremental hash functions <i>require</i> calling
 * {@link #reset} to reinitialize the state between calling an output method and
 * an update method.
 */
public interface StatefulHash extends Hash {

    /**
     * Returns a new instance of this stateful hash function reset to the
     * initial state.
     * 
     * @return a new instance of this hash in the initial state
     */
    StatefulHash createNew();

    /**
     * Returns whether this hash function supports incremental hashing.
     * Incremental hashing allows calling the {@link #update} methods to
     * continue hashing using accumulated state after calling any of the output
     * methods (such as {@link #getBytes}). Non-incremental hash functions
     * require calling {@link #reset} to reinitialize the state between calling
     * an output method and an update method.
     * 
     * @return true if incremental hashing is supported, false if not
     */
    boolean supportsIncremental();

    /**
     * Resets this hash function to its initial state. Resetting the state is
     * required for non-incremental hash functions after any output methods have
     * been called.
     */
    void reset();

    /**
     * Updates the state of this hash function with the entire given input
     * array.
     * 
     * @param input the input array
     * @throws IllegalStateException if this hash function is not incremental
     *             but an output method has been called without an intervening
     *             call to {@link #reset}
     */
    void update(byte[] input);

    /**
     * Updates the state of this hash function with the given range of the given
     * input array.
     * 
     * @param input the input array
     * @param index the starting index of the first input byte
     * @param length the length of the input range
     * @throws IllegalArgumentException if length is negative
     * @throws IndexOutOfBoundsException if index is negative or
     *             {@code index + length} is greater than the array length
     * @throws IllegalStateException if this hash function is not incremental
     *             but an output method has been called without an intervening
     *             call to {@link #reset}
     */
    void update(byte[] input, int index, int length);

    /**
     * Updates the state of this hash function with the remaining contents of
     * the given input buffer. This method leaves the buffer position at the
     * limit.
     * 
     * @param input the input buffer
     * @throws IllegalStateException if this hash function is not incremental
     *             but an output method has been called without an intervening
     *             call to {@link #reset}
     */
    void update(ByteBuffer input);

    /**
     * Updates the state of this hash function with memory with the given
     * address and length. The arguments are generally not checked in any way
     * and will likely lead to a VM crash or undefined results if invalid.
     * 
     * @param address the base address of the input
     * @param length the length of the input
     * @throws UnsupportedOperationException if this function does not support
     *             unsafe memory access
     * @throws IllegalStateException if this hash function is not incremental
     *             but an output method has been called without an intervening
     *             call to {@link #reset}
     * @see #supportsUnsafe()
     */
    void update(long address, long length);

    /**
     * Returns a new byte array containing the output of this hash function. The
     * caller may freely modify the contents of the array.
     * 
     * @return a new byte array containing the hash output
     */
    byte[] getBytes();

    /**
     * Writes the output of this hash function into the given byte array at the
     * given offset.
     * 
     * @param output the destination array for the output
     * @param index the starting index of the first output byte
     * @param maxLength the maximum number of bytes to write
     * @return the number of bytes written
     * @throws IllegalArgumentException if {@code maxLength} is negative
     * @throws IndexOutOfBoundsException if {@code index} is negative or if
     *             {@code index + maxLength} is greater than the array length
     */
    int getBytes(byte[] output, int index, int maxLength);

    /**
     * Returns the first byte of the output of this hash function.
     * 
     * @return the first output byte
     */
    byte getByte();

    /**
     * Returns the first two bytes of the output of this hash function as a
     * little-endian {@code short}. If the output is less than two bytes, the
     * remaining bytes are set to 0.
     * 
     * @return the first two output bytes as a short
     */
    short getShort();

    /**
     * Returns the first four bytes of the output of this hash function as a
     * little-endian {@code int}. If the output is less than four bytes, the
     * remaining bytes are set to 0.
     * 
     * @return the first four output bytes as an int
     */
    int getInt();

    /**
     * Returns the first eight bytes of the output of this hash function as a
     * little-endian {@code long}. If the output is less than eight bytes, the
     * remaining bytes are set to 0.
     * 
     * @return the first eight output bytes as a long
     */
    long getLong();
}

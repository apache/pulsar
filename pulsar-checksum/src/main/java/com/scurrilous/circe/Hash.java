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

/**
 * Abstract hash function. Each actual hash function is provided using a
 * {@linkplain StatefulHash stateful} derived interface. Hash functions with an
 * output length that fits within an {@code int} or a {@code long} are also
 * generally provided using a {@linkplain StatelessHash stateless} derived
 * interface. Given a stateless hash object, a method is provided for obtaining
 * a new corresponding stateful object.
 */
public interface Hash {

    /**
     * Returns the canonical name of this hash algorithm.
     * 
     * @return the name of this hash algorithm
     */
    String algorithm();

    /**
     * Returns the length in bytes of the output of this hash function.
     * 
     * @return the hash length in bytes
     */
    int length();

    /**
     * Returns whether this hash function supports unsafe access to arbitrary
     * memory addresses using methods such as
     * {@link StatefulHash#update(long, long)},
     * {@link StatelessIntHash#calculate(long, long)}, or
     * {@link IncrementalIntHash#resume(int, long, long)}. Such functions are
     * generally implemented in native code.
     * 
     * @return true if unsafe access is supported, false if not
     */
    boolean supportsUnsafe();
}

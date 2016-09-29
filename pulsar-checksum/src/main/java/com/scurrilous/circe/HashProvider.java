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

import java.util.EnumSet;

/**
 * Interface used to obtain instances of various kinds of hash algorithms.
 */
public interface HashProvider {

    /**
     * Returns information about the available implementations corresponding to
     * the given hash algorithm parameters.
     * 
     * @param params the hash algorithm parameters
     * @return a set of flags indicating the level of support
     */
    EnumSet<HashSupport> querySupport(HashParameters params);

    /**
     * Creates a stateful hash function using the given parameters.
     * 
     * @param params the hash algorithm parameters
     * @return a stateful hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    StatefulHash createStateful(HashParameters params);

    /**
     * Requests a stateless, int-width hash function with the given parameters.
     * Because not all stateless hash functions are incremental, this method may
     * be able to return implementations not supported by or more optimized than
     * {@link #getIncrementalInt}.
     * 
     * @param params the hash algorithm parameters
     * @return a stateless int-width hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    StatelessIntHash getStatelessInt(HashParameters params);

    /**
     * Requests a stateless, long-width hash function with the given parameters.
     * Because not all stateless hash functions are incremental, this method may
     * be able to return implementations not supported by or more optimized than
     * {@link #getIncrementalLong}.
     * <p>
     * Note that this method may return a less efficient hash function than
     * {@link #getStatelessInt} for hashes of 32 bits or less.
     * 
     * @param params the hash algorithm parameters
     * @return a stateless long-width hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    StatelessLongHash getStatelessLong(HashParameters params);

    /**
     * Requests an incremental, stateless, int-width hash function with the
     * given parameters. Note that although an algorithm may be available in
     * incremental form, some potentially more optimized implementations may not
     * support that form, and therefore cannot be provided be this method.
     * 
     * @param params the hash algorithm parameters
     * @return a stateful int-width hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    IncrementalIntHash getIncrementalInt(HashParameters params);

    /**
     * Requests an incremental, stateless, long-width hash function with the
     * given parameters. Note that although an algorithm may be available in
     * incremental form, some potentially more optimized implementations may not
     * support that form, and therefore cannot be provided be this method.
     * <p>
     * Also note that this method may return a less efficient hash function than
     * {@link #getIncrementalInt} for hashes of 32 bits or less.
     * 
     * @param params the hash algorithm parameters
     * @return a stateful long-width hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    IncrementalLongHash getIncrementalLong(HashParameters params);
}

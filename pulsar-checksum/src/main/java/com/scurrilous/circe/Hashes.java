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

import com.scurrilous.circe.HashParameters;
import com.scurrilous.circe.HashProviders;
import com.scurrilous.circe.HashSupport;
import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.IncrementalLongHash;
import com.scurrilous.circe.StatefulHash;
import com.scurrilous.circe.StatelessIntHash;
import com.scurrilous.circe.StatelessLongHash;

/**
 * Static methods to obtain various forms of abstract hash functions. Each
 * method uses {@link HashProviders#best} to find the best provider for the
 * given parameters and hash interface, and then calls the corresponding method
 * on that provider.
 */
public final class Hashes {

    private Hashes() {
    }

    /**
     * Creates a stateful hash function using the given parameters.
     * 
     * @param params the hash algorithm parameters
     * @return a stateful hash function
     * @throws UnsupportedOperationException if no provider supports the
     *             parameters
     */
    public static StatefulHash createStateful(HashParameters params) {
        return HashProviders.best(params).createStateful(params);
    }

    /**
     * Requests a stateless, int-width hash function with the given parameters.
     * Because not all stateless hash functions are incremental, this method may
     * be able to return implementations not supported by or more optimized than
     * {@link #getIncrementalInt}.
     * 
     * @param params the hash algorithm parameters
     * @return a stateless int-width hash function
     * @throws UnsupportedOperationException if no provider supports the
     *             parameters as a {@link StatelessIntHash}
     */
    public static StatelessIntHash getStatelessInt(HashParameters params) {
        return HashProviders.best(params, EnumSet.of(HashSupport.INT_SIZED))
                .getStatelessInt(params);
    }

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
     * @throws UnsupportedOperationException if no provider supports the
     *             parameters as a {@link StatelessLongHash}
     */
    public static StatelessLongHash getStatelessLong(HashParameters params) {
        return HashProviders.best(params, EnumSet.of(HashSupport.LONG_SIZED)).getStatelessLong(
                params);
    }

    /**
     * Requests an incremental, stateless, int-width hash function with the
     * given parameters. Note that although an algorithm may be available in
     * incremental form, some potentially more optimized implementations may not
     * support that form, and therefore cannot be provided be this method.
     * 
     * @param params the hash algorithm parameters
     * @return a stateful int-width hash function
     * @throws UnsupportedOperationException if no provider supports the
     *             parameters as an {@link IncrementalIntHash}
     */
    public static IncrementalIntHash getIncrementalInt(HashParameters params) {
        return HashProviders.best(params,
                EnumSet.of(HashSupport.INT_SIZED, HashSupport.STATELESS_INCREMENTAL))
                .getIncrementalInt(params);
    }

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
     * @throws UnsupportedOperationException if no provider supports the
     *             parameters as an {@link IncrementalLongHash}
     */
    public static IncrementalLongHash getIncrementalLong(HashParameters params) {
        return HashProviders.best(params,
                EnumSet.of(HashSupport.LONG_SIZED, HashSupport.STATELESS_INCREMENTAL))
                .getIncrementalLong(params);
    }
}

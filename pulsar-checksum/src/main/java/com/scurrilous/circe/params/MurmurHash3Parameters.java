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
package com.scurrilous.circe.params;

import com.scurrilous.circe.HashParameters;

/**
 * Hash parameters for <a
 * href="https://code.google.com/p/smhasher/wiki/MurmurHash3">MurmurHash3</a>.
 */
public final class MurmurHash3Parameters implements HashParameters {

    private final MurmurHash3Variant variant;
    private final int seed;

    /**
     * Constructs a {@link MurmurHash3Parameters} with the given variant and a
     * seed value of zero.
     * 
     * @param variant the variant of the algorithm
     */
    public MurmurHash3Parameters(MurmurHash3Variant variant) {
        this(variant, 0);
    }

    /**
     * Constructs a {@link MurmurHash3Parameters} with the given variant and
     * seed value.
     * 
     * @param variant the variant of the algorithm
     * @param seed the seed value
     */
    public MurmurHash3Parameters(MurmurHash3Variant variant, int seed) {
        this.variant = variant;
        this.seed = seed;
    }

    /**
     * Returns the variant of the hash algorithm.
     * 
     * @return the algorithm variant
     */
    public MurmurHash3Variant variant() {
        return variant;
    }

    /**
     * Returns the seed value for the hash function.
     * 
     * @return the seed value
     */
    public int seed() {
        return seed;
    }

    @Override
    public String algorithm() {
        return variant.algorithm();
    }
}

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
 * Hash parameters for <a href="https://131002.net/siphash/">SipHash-2-4</a>.
 */
public final class SipHash24Parameters implements HashParameters {

    private final long seedLow;
    private final long seedHigh;

    /**
     * Constructs a {@link SipHash24Parameters} with the default seed,
     * {@code 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F}.
     */
    public SipHash24Parameters() {
        this(0x0706050403020100L, 0x0f0e0d0c0b0a0908L);
    }

    /**
     * Constructs a {@link SipHash24Parameters} with the given seed.
     * 
     * @param seedLow the low-order 64 bits of the seed
     * @param seedHigh the high-order 64 bits of the seed
     */
    public SipHash24Parameters(long seedLow, long seedHigh) {
        this.seedLow = seedLow;
        this.seedHigh = seedHigh;
    }

    /**
     * Returns the low-order 64 bits of the seed.
     * 
     * @return low-order bits of seed
     */
    public long seedLow() {
        return seedLow;
    }

    /**
     * Returns the high-order 64 bits of the seed.
     * 
     * @return high-order bits of seed
     */
    public long seedHigh() {
        return seedHigh;
    }

    @Override
    public String algorithm() {
        return "SipHash-2-4";
    }
}

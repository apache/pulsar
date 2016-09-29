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
package com.scurrilous.circe.crc;

import com.scurrilous.circe.impl.AbstractIncrementalIntHash;

/**
 * Base implementation of int-width CRC functions.
 */
abstract class AbstractIntCrc extends AbstractIncrementalIntHash {

    private final String algorithm;
    protected final int bitWidth;
    private final int initial;
    private final int xorOut;

    AbstractIntCrc(String algorithm, int bitWidth, int initial, int xorOut) {
        if (bitWidth < 1 || bitWidth > 32)
            throw new IllegalArgumentException("invalid CRC width");
        this.algorithm = algorithm;
        this.bitWidth = bitWidth;
        this.initial = initial;
        this.xorOut = xorOut;
    }

    @Override
    public String algorithm() {
        return algorithm;
    }

    @Override
    public int length() {
        return (bitWidth + 7) / 8;
    }

    @Override
    protected int initial() {
        return initial ^ xorOut;
    }

    @Override
    protected int resumeUnchecked(int current, byte[] input, int index, int length) {
        return resumeRaw(current ^ xorOut, input, index, length) ^ xorOut;
    }

    protected abstract int resumeRaw(int crc, byte[] input, int index, int length);

    protected final int reflect(int value) {
        return Integer.reverse(value) >>> (32 - bitWidth);
    }
}

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

import com.scurrilous.circe.StatefulLongHash;
import com.scurrilous.circe.StatelessIntHash;
import com.scurrilous.circe.StatelessLongHash;

/**
 * Promotes a {@link StatelessIntHash} to a {@link StatelessLongHash}.
 */
public final class IntStatelessLongHash implements StatelessLongHash {

    private final StatelessIntHash intHash;

    /**
     * Constructs a new {@link IntStatelessLongHash} that delegates to the given
     * {@link StatelessIntHash}.
     * 
     * @param intHash the underlying int-width hash
     */
    public IntStatelessLongHash(StatelessIntHash intHash) {
        this.intHash = intHash;
    }

    @Override
    public String algorithm() {
        return intHash.algorithm();
    }

    @Override
    public int length() {
        return intHash.length();
    }

    @Override
    public boolean supportsUnsafe() {
        return intHash.supportsUnsafe();
    }

    @Override
    public StatefulLongHash createStateful() {
        return new IntStatefulLongHash(intHash.createStateful());
    }

    @Override
    public long calculate(byte[] input) {
        return intHash.calculate(input);
    }

    @Override
    public long calculate(byte[] input, int index, int length) {
        return intHash.calculate(input, index, length);
    }

    @Override
    public long calculate(ByteBuffer input) {
        return intHash.calculate(input);
    }

    @Override
    public long calculate(long address, long length) {
        return intHash.calculate(address, length);
    }
}

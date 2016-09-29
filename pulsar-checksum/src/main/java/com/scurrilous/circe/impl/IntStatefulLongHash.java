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

import com.scurrilous.circe.StatefulHash;
import com.scurrilous.circe.StatefulIntHash;
import com.scurrilous.circe.StatefulLongHash;
import com.scurrilous.circe.StatelessLongHash;

/**
 * Promotes a {@link StatefulIntHash} to a {@link StatefulLongHash}.
 */
public final class IntStatefulLongHash implements StatefulLongHash {

    private final StatefulIntHash intHash;

    /**
     * Constructs a new {@link IntStatefulLongHash} that delegates to the given
     * {@link StatefulIntHash}.
     * 
     * @param intHash the underlying int-width hash
     */
    public IntStatefulLongHash(StatefulIntHash intHash) {
        this.intHash = intHash;
    }

    public StatelessLongHash asStateless() {
        return new IntStatelessLongHash(intHash.asStateless());
    }

    public String algorithm() {
        return intHash.algorithm();
    }

    public int length() {
        return intHash.length();
    }

    public StatefulHash createNew() {
        return intHash.createNew();
    }

    public boolean supportsUnsafe() {
        return intHash.supportsUnsafe();
    }

    public boolean supportsIncremental() {
        return intHash.supportsIncremental();
    }

    public void reset() {
        intHash.reset();
    }

    public void update(byte[] input) {
        intHash.update(input);
    }

    public void update(byte[] input, int index, int length) {
        intHash.update(input, index, length);
    }

    public void update(ByteBuffer input) {
        intHash.update(input);
    }

    public void update(long address, long length) {
        intHash.update(address, length);
    }

    public byte[] getBytes() {
        return intHash.getBytes();
    }

    public int getBytes(byte[] output, int index, int maxLength) {
        return intHash.getBytes(output, index, maxLength);
    }

    public byte getByte() {
        return intHash.getByte();
    }

    public short getShort() {
        return intHash.getShort();
    }

    public int getInt() {
        return intHash.getInt();
    }

    public long getLong() {
        return intHash.getLong();
    }
}

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
import com.scurrilous.circe.StatelessIntHash;

class IncrementalIntStatefulHash extends AbstractStatefulHash implements StatefulIntHash {

    final AbstractIncrementalIntHash stateless;
    int current;

    IncrementalIntStatefulHash(AbstractIncrementalIntHash stateless) {
        this.stateless = stateless;
    }

    @Override
    public StatelessIntHash asStateless() {
        return stateless;
    }

    @Override
    public String algorithm() {
        return stateless.algorithm();
    }

    @Override
    public int length() {
        return stateless.length();
    }

    @Override
    public boolean supportsUnsafe() {
        return stateless.supportsUnsafe();
    }

    @Override
    public StatefulHash createNew() {
        return new IncrementalIntStatefulHash(stateless);
    }

    @Override
    public boolean supportsIncremental() {
        return true;
    }

    @Override
    public void reset() {
        current = stateless.initial();
    }

    @Override
    public void update(ByteBuffer input) {
        current = stateless.resume(current, input);
    }

    @Override
    public void update(long address, long length) {
        current = stateless.resume(current, address, length);
    }

    @Override
    protected void updateUnchecked(byte[] input, int index, int length) {
        current = stateless.resumeUnchecked(current, input, index, length);
    }

    @Override
    public int getInt() {
        return current;
    }

    @Override
    public long getLong() {
        return current;
    }
}

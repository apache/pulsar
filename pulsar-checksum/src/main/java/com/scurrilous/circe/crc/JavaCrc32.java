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

import java.util.zip.CRC32;

import com.scurrilous.circe.StatefulHash;
import com.scurrilous.circe.StatefulIntHash;
import com.scurrilous.circe.StatelessIntHash;
import com.scurrilous.circe.impl.AbstractStatefulHash;
import com.scurrilous.circe.impl.AbstractStatelessIntHash;
import com.scurrilous.circe.params.CrcParameters;

/**
 * Wraps {@link CRC32} in a {@link StatefulIntHash}.
 */
final class JavaCrc32 extends AbstractStatefulHash implements StatefulIntHash {

    private static final String ALGORITHM = CrcParameters.CRC32.algorithm();
    private static final int LENGTH = 4;

    private final CRC32 impl = new CRC32();

    @Override
    public String algorithm() {
        return ALGORITHM;
    }

    @Override
    public int length() {
        return LENGTH;
    }

    @Override
    public StatefulHash createNew() {
        return new JavaCrc32();
    }

    @Override
    public boolean supportsIncremental() {
        return true;
    }

    @Override
    public void reset() {
        impl.reset();
    }

    @Override
    protected void updateUnchecked(byte[] input, int index, int length) {
        impl.update(input, index, length);
    }

    @Override
    public int getInt() {
        return (int) impl.getValue();
    }

    @Override
    public long getLong() {
        return impl.getValue();
    }

    @Override
    public StatelessIntHash asStateless() {
        return new AbstractStatelessIntHash() {
            @Override
            public String algorithm() {
                return ALGORITHM;
            }

            @Override
            public int length() {
                return LENGTH;
            }

            @Override
            public StatefulIntHash createStateful() {
                return new JavaCrc32();
            }

            @Override
            protected int calculateUnchecked(byte[] input, int index, int length) {
                final CRC32 crc32 = new CRC32();
                crc32.update(input, index, length);
                return (int) crc32.getValue();
            }
        };
    }
}

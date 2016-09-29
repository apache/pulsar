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

import java.util.EnumSet;

import com.scurrilous.circe.Hash;
import com.scurrilous.circe.HashSupport;
import com.scurrilous.circe.StatelessHash;
import com.scurrilous.circe.impl.AbstractHashProvider;
import com.scurrilous.circe.params.CrcParameters;

/**
 * Provides pure Java and JDK-supplied CRC implementations.
 */
public final class StandardCrcProvider extends AbstractHashProvider<CrcParameters> {

    /**
     * Constructs a new {@link StandardCrcProvider}.
     */
    public StandardCrcProvider() {
        super(CrcParameters.class);
    }

    @Override
    protected EnumSet<HashSupport> querySupportTyped(CrcParameters params) {
        final EnumSet<HashSupport> result = EnumSet.of(HashSupport.STATEFUL,
                HashSupport.INCREMENTAL, HashSupport.STATELESS_INCREMENTAL, HashSupport.LONG_SIZED);
        if (params.bitWidth() <= 32)
            result.add(HashSupport.INT_SIZED);
        if (params.equals(CrcParameters.CRC32))
            result.add(HashSupport.NATIVE);
        return result;
    }

    @Override
    protected Hash get(CrcParameters params, EnumSet<HashSupport> required) {
        if (!required.contains(HashSupport.STATELESS_INCREMENTAL) &&
                params.equals(CrcParameters.CRC32))
            return new JavaCrc32();
        if (required.contains(HashSupport.NATIVE))
            throw new UnsupportedOperationException();
        return getCacheable(params, required);
    }

    @Override
    protected StatelessHash createCacheable(CrcParameters params, EnumSet<HashSupport> required) {
        final int bitWidth = params.bitWidth();
        if (bitWidth > 32 || (required.contains(HashSupport.LONG_SIZED) && bitWidth >= 8)) {
            if (required.contains(HashSupport.INT_SIZED))
                throw new UnsupportedOperationException();
            if (params.reflected())
                return new ReflectedLongCrc(params.algorithm(), bitWidth, params.polynomial(),
                        params.initial(), params.xorOut());
            else
                return new NormalLongCrc(params.algorithm(), bitWidth, params.polynomial(),
                        params.initial(), params.xorOut());
        } else {
            if (params.reflected())
                return new ReflectedIntCrc(params.algorithm(), bitWidth, (int) params.polynomial(),
                        (int) params.initial(), (int) params.xorOut());
            else if (bitWidth > 8)
                return new NormalIntCrc(params.algorithm(), bitWidth, (int) params.polynomial(),
                        (int) params.initial(), (int) params.xorOut());
            return new NormalByteCrc(params.algorithm(), bitWidth, (int) params.polynomial(),
                    (int) params.initial(), (int) params.xorOut());
        }
    }
}

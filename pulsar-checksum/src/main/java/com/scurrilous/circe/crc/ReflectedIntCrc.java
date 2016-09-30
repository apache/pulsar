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

/**
 * Implements a "reflected" LSB-first int-width CRC function using a lookup
 * table.
 */
final class ReflectedIntCrc extends AbstractIntCrc {

    private final int[] table = new int[256];

    ReflectedIntCrc(String algorithm, int width, int poly, int init, int xorOut) {
        super(algorithm, width, init, xorOut);

        poly = reflect(poly);
        for (int i = 0; i < 256; ++i) {
            int crc = i;
            for (int j = 0; j < 8; ++j)
                crc = (crc & 1) != 0 ? (crc >>> 1) ^ poly : crc >>> 1;
            table[i] = crc;
        }
    }

    @Override
    protected int initial() {
        return reflect(super.initial());
    }

    @Override
    protected int resumeRaw(int crc, byte[] input, int index, int length) {
        for (int i = 0; i < length; ++i)
            crc = table[(crc ^ input[index + i]) & 0xff] ^ (crc >>> 8);
        return crc;
    }
}

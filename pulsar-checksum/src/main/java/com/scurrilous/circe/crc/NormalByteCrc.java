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
 * Implements a "normal" MSB-first byte-width CRC function using a lookup table.
 */
final class NormalByteCrc extends AbstractIntCrc {

    private final byte[] table = new byte[256];

    NormalByteCrc(String algorithm, int bitWidth, int poly, int init, int xorOut) {
        super(algorithm, bitWidth, init, xorOut);
        if (bitWidth > 8)
            throw new IllegalArgumentException("invalid CRC width");

        final int widthMask = (1 << bitWidth) - 1;
        final int shpoly = poly << (8 - bitWidth);
        for (int i = 0; i < 256; ++i) {
            int crc = i;
            for (int j = 0; j < 8; ++j)
                crc = (crc & 0x80) != 0 ? (crc << 1) ^ shpoly : crc << 1;
            table[i] = (byte) ((crc >> (8 - bitWidth)) & widthMask);
        }
    }

    @Override
    protected int resumeRaw(int crc, byte[] input, int index, int length) {
        for (int i = 0; i < length; ++i)
            crc = table[(crc << (8 - bitWidth)) ^ (input[index + i] & 0xff)] & 0xff;
        return crc;
    }
}

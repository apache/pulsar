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
 * Hash parameters used to define a <a
 * href="http://en.wikipedia.org/wiki/Cyclic_redundancy_check">cyclic redundancy
 * check</a> (CRC). Includes some commonly used sets of parameters.
 */
public class CrcParameters implements HashParameters {

    private final String name;
    private final int bitWidth;
    private final long polynomial;
    private final long initial;
    private final long xorOut;
    private final boolean reflected;

    /**
     * Constructs a {@link CrcParameters} with the given parameters.
     * 
     * @param name the canonical name of the CRC function
     * @param bitWidth the width of the CRC function
     * @param polynomial the polynomial in binary form (non-reflected)
     * @param initial the initial value of the CRC register
     * @param xorOut a value XOR'ed with the CRC when it is read
     * @param reflected indicates whether the CRC is reflected (LSB-first)
     * @throws IllegalArgumentException if the width is less than 1 or greater
     *             than 64
     */
    public CrcParameters(String name, int bitWidth, long polynomial, long initial, long xorOut,
            boolean reflected) {
        if (bitWidth < 1 || bitWidth > 64)
            throw new IllegalArgumentException();
        this.name = name;
        this.bitWidth = bitWidth;
        final long mask = bitWidth < 64 ? (1L << bitWidth) - 1 : ~0L;
        this.polynomial = polynomial & mask;
        this.initial = initial & mask;
        this.xorOut = xorOut & mask;
        this.reflected = reflected;
    }

    @Override
    public String algorithm() {
        return name;
    }

    /**
     * Returns the width in bits of the CRC function. The width is also the
     * position of the implicit set bit at the top of the polynomial.
     * 
     * @return the CRC width in bits
     */
    public int bitWidth() {
        return bitWidth;
    }

    /**
     * Returns the binary form of polynomial that defines the CRC function (with
     * the implicit top bit omitted). For instance, the CRC-16 polynomial
     * x<sup>16</sup> + x<sup>15</sup> + x<sup>2</sup> + 1 is represented as
     * {@code 1000 0000 0000 0101} ({@code 0x8005}).
     * 
     * @return the CRC polynomial
     */
    public long polynomial() {
        return polynomial;
    }

    /**
     * Returns the initial value of the CRC register.
     * 
     * @return the CRC initial value
     */
    public long initial() {
        return initial;
    }

    /**
     * Returns the value XOR'ed with the CRC register when it is read to
     * determine the output value.
     * 
     * @return the final XOR value
     */
    public long xorOut() {
        return xorOut;
    }

    /**
     * Returns whether the CRC function is "reflected". Reflected CRCs process
     * data LSB-first, whereas "normal" CRCs are MSB-first.
     * 
     * @return whether the CRC function is reflected
     */
    public boolean reflected() {
        return reflected;
    }

    /**
     * Returns whether this object matches the given CRC parameters.
     * 
     * @param bitWidth the width of the CRC function
     * @param polynomial the polynomial in binary form (non-reflected)
     * @param initial the initial value of the CRC register
     * @param xorOut a value XOR'ed with the CRC when it is read
     * @param reflected indicates whether the CRC is reflected (LSB-first)
     * @return true if all parameters match exactly, false otherwise
     */
    public boolean match(int bitWidth, long polynomial, long initial, long xorOut, boolean reflected) {
        return bitWidth == this.bitWidth && polynomial == this.polynomial &&
                initial == this.initial && xorOut == this.xorOut && reflected == this.reflected;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        final CrcParameters other = (CrcParameters) obj;
        return bitWidth == other.bitWidth && polynomial == other.polynomial &&
                initial == other.initial && xorOut == other.xorOut && reflected == other.reflected;
    }

    @Override
    public int hashCode() {
        return (int) (polynomial ^ (polynomial >>> 32) ^ initial ^ (initial >>> 32) ^ xorOut ^ (xorOut >>> 32)) ^
                (reflected ? ~0 : 0);
    }

    /**
     * Parameters for CRC-16, used in the ARC and LHA compression utilities.
     */
    public static final CrcParameters CRC16 = new CrcParameters("CRC-16", 16, 0x8005, 0, 0, true);

    /**
     * Parameters for CRC-16/CCITT, used in the Kermit protocol.
     */
    public static final CrcParameters CRC16_CCITT = new CrcParameters("CRC-16/CCITT", 16, 0x1021,
            0, 0, true);

    /**
     * Parameters for CRC-16/XMODEM, used in the XMODEM protocol.
     */
    public static final CrcParameters CRC16_XMODEM = new CrcParameters("CRC-16/XMODEM", 16, 0x1021,
            0, 0, false);

    /**
     * Parameters for CRC-32, used in Ethernet, SATA, PKZIP, ZMODEM, etc.
     */
    public static final CrcParameters CRC32 = new CrcParameters("CRC-32", 32, 0x04c11db7, ~0, ~0,
            true);

    /**
     * Parameters for CRC-32/BZIP2, used in BZIP2.
     */
    public static final CrcParameters CRC32_BZIP2 = new CrcParameters("CRC-32/BZIP2", 32,
            0x04c11db7, ~0, ~0, false);

    /**
     * Parameters for CRC-32C, used in iSCSI and SCTP.
     */
    public static final CrcParameters CRC32C = new CrcParameters("CRC-32C", 32, 0x1edc6f41, ~0, ~0,
            true);

    /**
     * Parameters for CRC-32/MPEG-2, used in MPEG-2.
     */
    public static final CrcParameters CRC32_MPEG2 = new CrcParameters("CRC-32/MPEG-2", 32,
            0x04c11db7, ~0, 0, false);

    /**
     * Parameters for CRC-32/POSIX, used in the {@code cksum} utility.
     */
    public static final CrcParameters CRC32_POSIX = new CrcParameters("CRC-32/POSIX", 32,
            0x04c11db7, 0, ~0, false);

    /**
     * Parameters for CRC-64, used in the ECMA-182 standard for DLT-1 tapes.
     */
    public static final CrcParameters CRC64 = new CrcParameters("CRC-64", 64, 0x42f0e1eba9ea3693L,
            0L, 0L, false);

    /**
     * Parameters for CRC-64/XZ, used in the {@code .xz} file format.
     */
    public static final CrcParameters CRC64_XZ = new CrcParameters("CRC-64/XZ", 64,
            0x42f0e1eba9ea3693L, ~0L, ~0L, true);
}

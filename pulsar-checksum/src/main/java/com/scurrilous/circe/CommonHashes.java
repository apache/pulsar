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
package com.scurrilous.circe;

import com.scurrilous.circe.params.CrcParameters;
import com.scurrilous.circe.params.MurmurHash3Parameters;
import com.scurrilous.circe.params.MurmurHash3Variant;
import com.scurrilous.circe.params.SimpleHashParameters;
import com.scurrilous.circe.params.SipHash24Parameters;

/**
 * Static methods to obtain commonly-used hash functions. Note that a suitable
 * provider JAR must be made available on the class path. This class does not
 * have direct access to specific implementations; it simply constructs the hash
 * parameters and uses the {@link Hashes} class to search for a provider.
 */
public final class CommonHashes {

    private CommonHashes() {
    }

    /**
     * Returns an incremental stateless hash function implementing the
     * {@linkplain CrcParameters#CRC32 CRC-32} checksum algorithm.
     * 
     * @return a CRC-32 stateless incremental integer hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static IncrementalIntHash crc32() {
        return Hashes.getIncrementalInt(CrcParameters.CRC32);
    }

    /**
     * Returns an incremental stateless hash function implementing the
     * {@linkplain CrcParameters#CRC32C CRC-32C} checksum algorithm.
     * 
     * @return a CRC-32C stateless incremental integer hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static IncrementalIntHash crc32c() {
        return Hashes.getIncrementalInt(CrcParameters.CRC32C);
    }

    /**
     * Returns an incremental stateless hash function implementing the
     * {@linkplain CrcParameters#CRC64 CRC-64} checksum algorithm.
     * 
     * @return a CRC-64 stateless incremental long integer hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static IncrementalLongHash crc64() {
        return Hashes.getIncrementalLong(CrcParameters.CRC64);
    }

    /**
     * Returns a hash function implementing the MurmurHash3 algorithm, 32-bit
     * x86 variant, with a seed of 0.
     * 
     * @return a MurmurHash3 32-bit/x86 stateless integer hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatelessIntHash murmur3_32() {
        return Hashes.getStatelessInt(new MurmurHash3Parameters(MurmurHash3Variant.X86_32));
    }

    /**
     * Returns a hash function implementing the MurmurHash3 algorithm, 32-bit
     * x86 variant, with the given seed value
     * 
     * @param seed the 32-bit seed value
     * @return a MurmurHash3 32-bit/x86 stateless integer hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatelessIntHash murmur3_32(int seed) {
        return Hashes.getStatelessInt(new MurmurHash3Parameters(MurmurHash3Variant.X86_32, seed));
    }

    /**
     * Returns a hash function implementing the MurmurHash3 algorithm, 128-bit
     * x64 variant, with a seed of 0.
     * 
     * @return a MurmurHash3 128-bit/x64 stateful hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatefulHash murmur3_128() {
        return Hashes.createStateful(new MurmurHash3Parameters(MurmurHash3Variant.X64_128));
    }

    /**
     * Returns a hash function implementing the MurmurHash3 algorithm, 128-bit
     * x64 variant, with the given seed value.
     * 
     * @param seed the 32-bit seed value
     * @return a MurmurHash3 128-bit/x64 stateful hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatefulHash murmur3_128(int seed) {
        return Hashes.createStateful(new MurmurHash3Parameters(MurmurHash3Variant.X64_128, seed));
    }

    /**
     * Returns a hash function implementing the SipHash-2-4 algorithm (64 bits)
     * with the default seed,
     * {@code 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F}.
     * 
     * @return a SipHash-2-4 stateless long integer hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatelessLongHash sipHash24() {
        return Hashes.getStatelessLong(new SipHash24Parameters());
    }

    /**
     * Returns a hash function implementing the SipHash-2-4 algorithm (64 bits)
     * with the given seed value.
     * 
     * @param seedLow the low-order 64 bits of the seed
     * @param seedHigh the high-order 64 bits of the seed
     * @return a SipHash-2-4 stateless long integer hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatelessLongHash sipHash24(long seedLow, long seedHigh) {
        return Hashes.getStatelessLong(new SipHash24Parameters(seedLow, seedHigh));
    }

    /**
     * Returns a hash function implementing the MD5 algorithm (128 bits).
     * 
     * @return an MD5 stateful hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatefulHash md5() {
        return Hashes.createStateful(SimpleHashParameters.MD5);
    }

    /**
     * Returns a hash function implementing the SHA-1 algorithm (160 bits).
     * 
     * @return a SHA-1 stateful hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatefulHash sha1() {
        return Hashes.createStateful(SimpleHashParameters.SHA1);
    }

    /**
     * Returns a hash function implementing the SHA-256 algorithm (256 bits).
     * 
     * @return a SHA-256 stateful hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatefulHash sha256() {
        return Hashes.createStateful(SimpleHashParameters.SHA256);
    }

    /**
     * Returns a hash function implementing the SHA-384 algorithm (384 bits).
     * 
     * @return a SHA-384 stateful hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatefulHash sha384() {
        return Hashes.createStateful(SimpleHashParameters.SHA384);
    }

    /**
     * Returns a hash function implementing the SHA-512 algorithm (512 bits).
     * 
     * @return a SHA-512 stateful hash
     * @throws UnsupportedOperationException if no provider is available
     */
    public static StatefulHash sha512() {
        return Hashes.createStateful(SimpleHashParameters.SHA512);
    }
}

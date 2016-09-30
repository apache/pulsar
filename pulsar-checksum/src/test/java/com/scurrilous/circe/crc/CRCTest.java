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

import static com.scurrilous.circe.params.CrcParameters.CRC16;
import static com.scurrilous.circe.params.CrcParameters.CRC16_CCITT;
import static com.scurrilous.circe.params.CrcParameters.CRC16_XMODEM;
import static com.scurrilous.circe.params.CrcParameters.CRC32;
import static com.scurrilous.circe.params.CrcParameters.CRC32C;
import static com.scurrilous.circe.params.CrcParameters.CRC32_BZIP2;
import static com.scurrilous.circe.params.CrcParameters.CRC32_POSIX;
import static com.scurrilous.circe.params.CrcParameters.CRC64;
import static com.scurrilous.circe.params.CrcParameters.CRC64_XZ;
import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;

import org.testng.annotations.Test;

import com.scurrilous.circe.HashProvider;
import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.params.CrcParameters;

/**
 * Tests the {@link StandardCrcProvider} with various CRC algorithms. See the <a
 * href="http://reveng.sourceforge.net/crc-catalogue/">Catalogue of parametrised
 * CRC algorithms</a> for more information on these algorithms and others.
 */
@SuppressWarnings("javadoc")
public class CRCTest {

    private static final HashProvider PROVIDER = new StandardCrcProvider();
    private static final Charset ASCII = Charset.forName("ASCII");
    private static final byte[] DIGITS = "123456789".getBytes(ASCII);

    @Test
    public void testCRC3_ROHC() {
        final CrcParameters CRC3_ROHC = new CrcParameters("CRC-3/ROHC", 3, 0x3, 0x7, 0, true);
        assertEquals(0x6, PROVIDER.getIncrementalInt(CRC3_ROHC).calculate(DIGITS));
    }

    @Test
    public void testCRC5_EPC() {
        final CrcParameters CRC5_EPC = new CrcParameters("CRC-5/EPC", 5, 0x09, 0x09, 0, false);
        assertEquals(0x00, PROVIDER.getIncrementalInt(CRC5_EPC).calculate(DIGITS));
    }

    @Test
    public void testCRC5_USB() {
        final CrcParameters CRC5_USB = new CrcParameters("CRC-5/USB", 5, 0x05, 0x1f, 0x1f, true);
        assertEquals(0x19, PROVIDER.getIncrementalInt(CRC5_USB).calculate(DIGITS));
    }

    @Test
    public void testCRC7() {
        final CrcParameters CRC7 = new CrcParameters("CRC-7", 7, 0x09, 0, 0, false);
        assertEquals(0x75, PROVIDER.getIncrementalInt(CRC7).calculate(DIGITS));
    }

    @Test
    public void testCRC7ROHC() {
        final CrcParameters CRC7_ROHC = new CrcParameters("CRC-7/ROHC", 7, 0x4f, 0x7f, 0, true);
        assertEquals(0x53, PROVIDER.getIncrementalInt(CRC7_ROHC).calculate(DIGITS));
    }

    @Test
    public void testCRC8() {
        final CrcParameters CRC8 = new CrcParameters("CRC-8", 8, 0x07, 0, 0, false);
        assertEquals(0xf4, PROVIDER.getIncrementalInt(CRC8).calculate(DIGITS));
    }

    @Test
    public void testCRC10() {
        final CrcParameters CRC10 = new CrcParameters("CRC-10", 10, 0x233, 0, 0, false);
        assertEquals(0x199, PROVIDER.getIncrementalInt(CRC10).calculate(DIGITS));
    }

    @Test
    public void testCRC15() {
        final CrcParameters CRC15 = new CrcParameters("CRC-15", 15, 0x4599, 0, 0, false);
        assertEquals(0x059e, PROVIDER.getIncrementalInt(CRC15).calculate(DIGITS));
    }

    @Test
    public void testCRC16() {
        assertEquals(0xbb3d, PROVIDER.getIncrementalInt(CRC16).calculate(DIGITS));
    }

    @Test
    public void testCRC16_CCITT() {
        assertEquals(0x2189, PROVIDER.getIncrementalInt(CRC16_CCITT).calculate(DIGITS));
    }

    @Test
    public void testXMODEM() {
        assertEquals(0x31c3, PROVIDER.getIncrementalInt(CRC16_XMODEM).calculate(DIGITS));
    }

    @Test
    public void testCRC24() {
        final CrcParameters CRC24 = new CrcParameters("CRC-24", 24, 0x864cfb, 0xb704ce, 0, false);
        assertEquals(0x21cf02, PROVIDER.getIncrementalInt(CRC24).calculate(DIGITS));
    }

    @Test
    public void testCRC32() {
        assertEquals(0xcbf43926, PROVIDER.getIncrementalInt(CRC32).calculate(DIGITS));
    }

    @Test
    public void testJavaCRC32() {
        assertEquals(0xcbf43926, PROVIDER.getStatelessInt(CRC32).calculate(DIGITS));
    }

    @Test
    public void testBZIP2() {
        assertEquals(0xfc891918, PROVIDER.getIncrementalInt(CRC32_BZIP2).calculate(DIGITS));
    }

    @Test
    public void testCRC32C() {
        assertEquals(0xe3069283, PROVIDER.getIncrementalInt(CRC32C).calculate(DIGITS));
    }

    @Test
    public void testCRC32D() {
        final CrcParameters CRC32D = new CrcParameters("CRC-32D", 32, 0xa833982b, ~0, ~0, true);
        assertEquals(0x87315576, PROVIDER.getIncrementalInt(CRC32D).calculate(DIGITS));
    }

    @Test
    public void testPOSIX() {
        assertEquals(0x765e7680, PROVIDER.getIncrementalInt(CRC32_POSIX).calculate(DIGITS));
    }

    @Test
    public void testCRC32Q() {
        final CrcParameters CRC32Q = new CrcParameters("CRC-32Q", 32, 0x814141ab, 0, 0, false);
        assertEquals(0x3010bf7f, PROVIDER.getIncrementalInt(CRC32Q).calculate(DIGITS));
    }

    @Test
    public void testCRC64() {
        assertEquals(0x6c40df5f0b497347L, PROVIDER.getIncrementalLong(CRC64).calculate(DIGITS));
    }

    @Test
    public void testCRC64_XZ() {
        assertEquals(0x995dc9bbdf1939faL, PROVIDER.getIncrementalLong(CRC64_XZ).calculate(DIGITS));
    }
    
    @Test
    public void testCRC32CIncremental() {
        // reflected
        testIncremental(PROVIDER.getIncrementalInt(CRC32C));
    }

    private void testIncremental(IncrementalIntHash hash) {
        final String data = "data";
        final String combined = data + data;

        final int dataChecksum = hash.calculate(data.getBytes(ASCII));
        final int combinedChecksum = hash.calculate(combined.getBytes(ASCII));
        final int incrementalChecksum = hash.resume(dataChecksum, data.getBytes(ASCII));
        assertEquals(combinedChecksum, incrementalChecksum);
    }
}
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

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;

import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class CommonHashesTest {

    private static final Charset ASCII = Charset.forName("ASCII");
    private static final byte[] DIGITS = "123456789".getBytes(ASCII);


    @Test
    public void testCrc32() {
        assertEquals(0xcbf43926, CommonHashes.crc32().calculate(DIGITS));
    }

    @Test
    public void testCrc32c() {
        assertEquals(0xe3069283, CommonHashes.crc32c().calculate(DIGITS));
    }

    @Test
    public void testCrc64() {
        assertEquals(0x6c40df5f0b497347L, CommonHashes.crc64().calculate(DIGITS));
    }
}

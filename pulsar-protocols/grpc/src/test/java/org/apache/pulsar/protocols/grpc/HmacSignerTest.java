/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.protocols.grpc;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class HmacSignerTest {

    @Test
    public void testNoSecret() {
        assertThrows(IllegalArgumentException.class, () -> new HmacSigner(null));
    }

    @Test
    public void testComputeSignature() {
        HmacSigner signer = new HmacSigner("secret".getBytes());
        byte[] s1 = signer.computeSignature("ok".getBytes());
        byte[] s2 = signer.computeSignature("ok".getBytes());
        byte[] s3 = signer.computeSignature("wrong".getBytes());
        assertEquals(s1.length, 32);
        assertEquals(s1, s2);
        assertEquals(s3.length, 32);
        assertNotEquals(s1, s3);
    }

    @Test
    public void testGeneratedKey() {
        HmacSigner signer = new HmacSigner();
        byte[] s1 = signer.computeSignature("ok".getBytes());
        assertEquals(s1.length, 32);
    }

    @Test
    public void testNullAndEmptyString() {
        HmacSigner signer = new HmacSigner("secret".getBytes());
        byte[] s1 = signer.computeSignature(null);
        assertEquals(s1.length, 32);
        s1 = signer.computeSignature("".getBytes());
        assertEquals(s1.length, 32);
    }
}

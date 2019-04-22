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

package org.apache.pulsar.broker.authentication;

import javax.naming.AuthenticationException;

import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class SaslServerTokenSignerTest {

    @Test
    public void testNoSecret() throws Exception {
        try {
            new SaslRoleTokenSigner(null);
            Assert.fail();
        }
        catch (IllegalArgumentException ex) {
        }
    }

    @Test
    public void testNullAndEmptyString() throws Exception {
        SaslRoleTokenSigner signer = new SaslRoleTokenSigner("secret".getBytes());
        try {
            signer.sign(null);
            Assert.fail("Null String should Failed");
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assert.fail("Null String should Failed with IllegalArgumentException.");
        }
        try {
            signer.sign("");
            Assert.fail("Empty String should Failed");
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assert.fail("Empty String should Failed with IllegalArgumentException.");
        }
    }

    @Test
    public void testSignature() throws Exception {
        SaslRoleTokenSigner signer = new SaslRoleTokenSigner("secret".getBytes());
        String s1 = signer.sign("ok");
        String s2 = signer.sign("ok");
        String s3 = signer.sign("wrong");
        Assert.assertEquals(s1, s2);
        Assert.assertNotSame(s1, s3);
    }

    @Test
    public void testVerify() throws Exception {
        SaslRoleTokenSigner signer = new SaslRoleTokenSigner("secret".getBytes());
        String t = "test";
        String s = signer.sign(t);
        String e = signer.verifyAndExtract(s);
        Assert.assertEquals(t, e);
        Assert.assertNotEquals(t, s);

    }

    @Test
    public void testInvalidSignedText() throws Exception {
        SaslRoleTokenSigner signer = new SaslRoleTokenSigner("secret".getBytes());
        try {
            signer.verifyAndExtract("test");
            Assert.fail();
        } catch (AuthenticationException ex) {
            // Expected
        } catch (Throwable ex) {
            Assert.fail();
        }
    }

    @Test
    public void testTampering() throws Exception {
        SaslRoleTokenSigner signer = new SaslRoleTokenSigner("secret".getBytes());
        String t = "test";
        String s = signer.sign(t);
        s += "x";
        try {
            signer.verifyAndExtract(s);
            Assert.fail();
        } catch (AuthenticationException ex) {
            // Expected
        } catch (Throwable ex) {
            Assert.fail();
        }
    }
}

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
package org.apache.pulsar.client.api;

import org.apache.pulsar.client.impl.auth.MockEncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.impl.auth.MockAuthentication;

import org.testng.annotations.Test;
import org.testng.Assert;

public class AuthenticationTest {

    @Test
    public void testConfigureDefaultFormat() {
        try {
            MockAuthentication testAuthentication =
                    (MockAuthentication) AuthenticationFactory.create("org.apache.pulsar.client.impl.auth.MockAuthentication",
                            "key1:value1,key2:value2");
            Assert.assertEquals(testAuthentication.authParamsMap.get("key1"), "value1");
            Assert.assertEquals(testAuthentication.authParamsMap.get("key2"), "value2");
        } catch (PulsarClientException.UnsupportedAuthenticationException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConfigureWrongFormat() {
        try {
            MockAuthentication testAuthentication =
                    (MockAuthentication) AuthenticationFactory.create(
                            "org.apache.pulsar.client.impl.auth.MockAuthentication",
                            "foobar");
            Assert.assertTrue(testAuthentication.authParamsMap.isEmpty());
        } catch (PulsarClientException.UnsupportedAuthenticationException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConfigureNull() {
        try {
            MockAuthentication testAuthentication = (MockAuthentication) AuthenticationFactory.create(
                    "org.apache.pulsar.client.impl.auth.MockAuthentication",
                    (String) null);
            Assert.assertTrue(testAuthentication.authParamsMap.isEmpty());
        } catch (PulsarClientException.UnsupportedAuthenticationException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConfigureEmpty() {
        try {
            MockAuthentication testAuthentication =
                    (MockAuthentication) AuthenticationFactory.create(
                            "org.apache.pulsar.client.impl.auth.MockAuthentication",
                            "");
            Assert.assertTrue(testAuthentication.authParamsMap.isEmpty());
        } catch (PulsarClientException.UnsupportedAuthenticationException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testConfigurePluginSide() {
        try {
            MockEncodedAuthenticationParameterSupport testAuthentication =
                    (MockEncodedAuthenticationParameterSupport) AuthenticationFactory.create(
                            "org.apache.pulsar.client.impl.auth.MockEncodedAuthenticationParameterSupport",
                            "key1:value1;key2:value2");
            Assert.assertEquals(testAuthentication.authParamsMap.get("key1"), "value1");
            Assert.assertEquals(testAuthentication.authParamsMap.get("key2"), "value2");
        } catch (PulsarClientException.UnsupportedAuthenticationException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}

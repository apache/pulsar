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
package org.apache.pulsar.client.impl.conf;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class DefaultCryptoKeyReaderConfigurationDataTest {

    @Test
    public void testClone() throws Exception {
        DefaultCryptoKeyReaderConfigurationData conf = new DefaultCryptoKeyReaderConfigurationData();
        conf.setDefaultPublicKey("file:///path/to/default-public.key");
        conf.setDefaultPrivateKey("file:///path/to/default-private.key");
        conf.setPublicKey("key1", "file:///path/to/public1.key");
        conf.setPrivateKey("key2", "file:///path/to/private2.key");
        DefaultCryptoKeyReaderConfigurationData clone = conf.clone();

        conf.setDefaultPublicKey("data:AAAAA");
        conf.setDefaultPrivateKey("data:BBBBB");
        conf.setPublicKey("key1", "data:CCCCC");
        conf.setPrivateKey("key2", "data:DDDDD");

        assertEquals(clone.getDefaultPublicKey(), "file:///path/to/default-public.key");
        assertEquals(clone.getDefaultPrivateKey(), "file:///path/to/default-private.key");
        assertEquals(clone.getPublicKeys().get("key1"), "file:///path/to/public1.key");
        assertEquals(clone.getPrivateKeys().get("key2"), "file:///path/to/private2.key");
    }

    @Test
    public void testToString() {
        DefaultCryptoKeyReaderConfigurationData conf = new DefaultCryptoKeyReaderConfigurationData();
        assertEquals(conf.toString(),
                "DefaultCryptoKeyReaderConfigurationData(defaultPublicKey=null, defaultPrivateKey=null, publicKeys={}, privateKeys={})");

        conf.setDefaultPublicKey("file:///path/to/default-public.key");
        conf.setDefaultPrivateKey("data:AAAAA");
        conf.setPublicKey("key1", "file:///path/to/public.key");
        conf.setPrivateKey("key2", "file:///path/to/private.key");
        assertEquals(conf.toString(),
                "DefaultCryptoKeyReaderConfigurationData(defaultPublicKey=file:///path/to/default-public.key, defaultPrivateKey=data:*****, publicKeys={key1=file:///path/to/public.key}, privateKeys={key2=file:///path/to/private.key})");

        conf.setPublicKey("key3", "data:BBBBB");
        conf.setPrivateKey("key4", "data:CCCCC");
        assertTrue(conf.toString().startsWith(
                "DefaultCryptoKeyReaderConfigurationData(defaultPublicKey=file:///path/to/default-public.key, defaultPrivateKey=data:*****, publicKeys={"));
        assertTrue(conf.toString().contains("key3=data:*****"));
        assertFalse(conf.toString().contains("key3=data:BBBBB"));
        assertTrue(conf.toString().contains("key4=data:*****"));
        assertFalse(conf.toString().contains("key4=data:CCCCC"));
        assertTrue(conf.toString().endsWith("})"));
    }

}

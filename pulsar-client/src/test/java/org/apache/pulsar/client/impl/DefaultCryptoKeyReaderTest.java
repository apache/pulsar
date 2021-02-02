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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class DefaultCryptoKeyReaderTest {

    @Test
    public void testBuild() throws Exception {
        Map<String, String> publicKeys = new HashMap<>();
        publicKeys.put("key1", "file:///path/to/public1.key");
        publicKeys.put("key2", "file:///path/to/public2.key");

        Map<String, String> privateKeys = new HashMap<>();
        privateKeys.put("key3", "file:///path/to/private3.key");

        DefaultCryptoKeyReader keyReader = DefaultCryptoKeyReader.builder()
                .defaultPublicKey("file:///path/to/default-public.key")
                .defaultPrivateKey("file:///path/to/default-private.key")
                .publicKey("key4", "file:///path/to/public4.key").publicKeys(publicKeys)
                .publicKey("key5", "file:///path/to/public5.key").privateKey("key6", "file:///path/to/private6.key")
                .privateKeys(privateKeys).privateKey("key7", "file:///path/to/private7.key").build();

        Field defaultPublicKeyField = keyReader.getClass().getDeclaredField("defaultPublicKey");
        defaultPublicKeyField.setAccessible(true);
        Field defaultPrivateKeyField = keyReader.getClass().getDeclaredField("defaultPrivateKey");
        defaultPrivateKeyField.setAccessible(true);
        Field publicKeysField = keyReader.getClass().getDeclaredField("publicKeys");
        publicKeysField.setAccessible(true);
        Field privateKeysField = keyReader.getClass().getDeclaredField("privateKeys");
        privateKeysField.setAccessible(true);

        Map<String, String> expectedPublicKeys = new HashMap<>();
        expectedPublicKeys.put("key1", "file:///path/to/public1.key");
        expectedPublicKeys.put("key2", "file:///path/to/public2.key");
        expectedPublicKeys.put("key4", "file:///path/to/public4.key");
        expectedPublicKeys.put("key5", "file:///path/to/public5.key");

        Map<String, String> expectedPrivateKeys = new HashMap<>();
        expectedPrivateKeys.put("key3", "file:///path/to/private3.key");
        expectedPrivateKeys.put("key6", "file:///path/to/private6.key");
        expectedPrivateKeys.put("key7", "file:///path/to/private7.key");

        assertEquals((String) defaultPublicKeyField.get(keyReader), "file:///path/to/default-public.key");
        assertEquals((String) defaultPrivateKeyField.get(keyReader), "file:///path/to/default-private.key");
        assertEquals((Map<String, String>) publicKeysField.get(keyReader), expectedPublicKeys);
        assertEquals((Map<String, String>) privateKeysField.get(keyReader), expectedPrivateKeys);
    }

    @Test
    public void testGetKeys() throws Exception {
        final String ecdsaPublicKey = "./src/test/resources/crypto_ecdsa_public.key";
        final String ecdsaPrivateKey = "./src/test/resources/crypto_ecdsa_private.key";
        final String rsaPublicKey = "./src/test/resources/crypto_rsa_public.key";
        final String rsaPrivateKey = "./src/test/resources/crypto_rsa_private.key";

        DefaultCryptoKeyReader keyReader1 = DefaultCryptoKeyReader.builder().build();
        assertNull(keyReader1.getPublicKey("key0", null).getKey());
        assertNull(keyReader1.getPrivateKey("key0", null).getKey());

        DefaultCryptoKeyReader keyReader2 = DefaultCryptoKeyReader.builder().defaultPublicKey("file:" + ecdsaPublicKey)
                .defaultPrivateKey("file:" + ecdsaPrivateKey)
                .publicKey("key1",
                        "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF6elRUenNTc1pGWWxXeWJack1OdwphRGpncWluSU5vNXlOa0h1UkJQZzJyNTZCRWFIb1U1eStjY0RoeXhCR0NLUFprVGNRYXN2WWdXSjNzSFJLQWxOCmRaTkc4R3QzazJTcmZEcnJ0ajFLTDNHNk5XUkE4VHF5Umt4eGw1dnBBTWM2OVVqWDlIUHdTemxtckM3WlhtMWUKU3dZVFY3Kzdxcy82OUpMQm5yTUpjc2wrSXlYVWFoaFJuOHcyRmtzOUpXcmlOS2kxUFNnQ1BqTWpnS0JGN3lhRQpBVEowR01TTWM4RnZYV3dGSnNXQldRa1V3Z3FsRXhSMU1EaVZWQnR3OVF0SkIyOUlOaTBORHMyUGViNjFEdDQ5Ck5abE4va2xKQ1hJVXRCU0lxZzlvK2lSS1Z3WExIbklNMFdIVm5tUm4yTUswbmYwMy9Ed0NJVm5iNWVsVG9aNzIKOHdJREFRQUIKLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg==") // crypto_rsa_public.key
                .privateKey("key1",
                        "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBenpUVHpzU3NaRllsV3liWnJNTndhRGpncWluSU5vNXlOa0h1UkJQZzJyNTZCRWFICm9VNXkrY2NEaHl4QkdDS1Baa1RjUWFzdllnV0ozc0hSS0FsTmRaTkc4R3QzazJTcmZEcnJ0ajFLTDNHNk5XUkEKOFRxeVJreHhsNXZwQU1jNjlValg5SFB3U3psbXJDN1pYbTFlU3dZVFY3Kzdxcy82OUpMQm5yTUpjc2wrSXlYVQphaGhSbjh3MkZrczlKV3JpTktpMVBTZ0NQak1qZ0tCRjd5YUVBVEowR01TTWM4RnZYV3dGSnNXQldRa1V3Z3FsCkV4UjFNRGlWVkJ0dzlRdEpCMjlJTmkwTkRzMlBlYjYxRHQ0OU5abE4va2xKQ1hJVXRCU0lxZzlvK2lSS1Z3WEwKSG5JTTBXSFZubVJuMk1LMG5mMDMvRHdDSVZuYjVlbFRvWjcyOHdJREFRQUJBb0lCQUhXRnZmaVJua0dPaHNPTApabnpSb01qTU1janh4OGdCeFErM0YxL3ZjbUkvRk0rbC9UbGxXRnNKSUp3allveEExZHFvaGRDTk9tTzdSblpjCnNiZW1oeE4veEFXS3ZwaVB5Wis5ZjRHdWc0d2pVZjBFYnIwamtJZkV4Y3k2dGs0bHNlLzdMOWxMaE9mMWw2RmoKTlJDVXNaMlZ4WlRJZjdXakh2Qm02SUNOaFhkZmdjL1RPWC9INEJCTXh5UWtrbXZTN3lRSFBtbmVrVnBDandYaQpSZ2RQT3BCU0hVQXN1TGMzY2RPN0R6U2xYQnkrUjNVQjViQzk3ZWZTVHd4bU1kY0dVTlFoMTdDdXcrb3UyT0xKCmwvV3lNQkpnS1AwenA4TUkyWUNQMHRvRTFWVjBGV2lzaU5VZHl3Mm1tZHNLQlBDdFpXNEpmL2F2UkxqQ3B5ODMKZ3llSGk0a0NnWUVBN2ZhYzh1L1dvVWNSeGZ3YmV4eFZOSWI1anBWZ1EyMlhYVXZjT0JBMzE0NUhGSDRrRDlRcwpPbE9NNDhpRVgxR3ErRk9iK1RrVmEzeWVFYnlFSFZtTnhtN1pxREVsR2xQbkhIZ1dKZlZvNGx0ZW1rTlE4Y1FJCkNpRGhVSDdEOWlHZDRUckxxK3U4Slkvb3kwZHBKeWFKL0dzTlB3alZ6TWlBOWtEdUkyS0tScGNDZ1lFQTN1bHAKc1p5ODJFaWJUK1lZTnNoZkF5elBjUWMrQ1ZkY3BTM3lFVU1jTXJyR1QySUhiQWhsZHpBV3ZuaTdQRjFDdllvQgplb1BTR2lFR01Bd0FmYVVJNHBzWnZBVFpUZitHV0tpemxIODIwbHc0dFkyTlcydFlGd0RjWjZFUEtkcTlaQ096CkxmeXcyTmhMcWkyRnBGeUFwY1hsQTkyVVVJMEZjWENDdEFLSjJnVUNnWUVBc0k1bWVyVktpTlRETWlOUWZISlUKSWFuM3BUdmRkWW50WVhKMGpVQXpQb0s0NkZLRERSOStSVFJTZDNzQ0Evc0RJRVpnbG5RdEdWZ1hxODgwTXRhTQpJMnVCb0pIK0ZsK2tQUEk0ZEtkMXoyUzlkelYwN0R4blBxU1FwL20yQ1h0OXVXdTNTL0tXNFVPNkZJRUNXdUwwClJFMWxRWnliak5wREhQS2wvYWtTTVRjQ2dZQnFDODBXakNSakdKZWF1VEpIemFjMTBYbVdvZ1ZuV0VKZzZxekEKZlpiS280UjRlNEJnYXRZcWo1d2lYVGxtREZBVjc3T29YMUh5MEVjclVHcGpXOElRWEEwd0gzWnAzdWhCQVhEOQpjay9ZWDdzeTAvYXR5VEdOTUFHcTR6cGRoUXlZdVVzaTA1WW1jeS83ODlBaVUwZDRsZDdQcWZoSEllKzIrZm1VClBhanJLUUtCZ1FDanBqMFkrRS9IaTBTSlVLTlZQbDN1K1NOMWxMY1IxL2dNakpic1lGR3VhMU1zTFhCTlhUU2wKUWlZSGlhZFQ3QmhQRWtGZFc3dStiRndzMmFkbVcxOUJvVWIrd2d0WlQvdDduVHlvUzRMYWc0dnlhek5QWnpkUQp4NlhQcndaaW1kMFhERGl0R0xqY0xmOTkxRUZzWFNxUHpuRERmWHRKMzErb1U2T2JuSFNJdEE9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=") // crypto_rsa_private.key
                .publicKey("key2", "file:invalid").privateKey("key2", "file:invalid").publicKey("key3", "data:invalid")
                .privateKey("key3", "data:invalid").build();

        assertNotNull(keyReader2.getPublicKey("key0", null).getKey());
        assertEquals(keyReader2.getPublicKey("key0", null).getKey(), Files.readAllBytes(Paths.get(ecdsaPublicKey)));
        assertNotNull(keyReader2.getPrivateKey("key0", null).getKey());
        assertEquals(keyReader2.getPrivateKey("key0", null).getKey(), Files.readAllBytes(Paths.get(ecdsaPrivateKey)));

        assertNotNull(keyReader2.getPublicKey("key1", null).getKey());
        assertEquals(keyReader2.getPublicKey("key1", null).getKey(), Files.readAllBytes(Paths.get(rsaPublicKey)));
        assertNotNull(keyReader2.getPrivateKey("key1", null).getKey());
        assertEquals(keyReader2.getPrivateKey("key1", null).getKey(), Files.readAllBytes(Paths.get(rsaPrivateKey)));

        assertNull(keyReader1.getPublicKey("key2", null).getKey());
        assertNull(keyReader1.getPrivateKey("key2", null).getKey());
        assertNull(keyReader1.getPublicKey("key3", null).getKey());
        assertNull(keyReader1.getPrivateKey("key3", null).getKey());
    }

}

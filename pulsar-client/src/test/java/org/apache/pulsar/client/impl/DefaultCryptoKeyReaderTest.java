/*
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
import org.apache.commons.lang3.SystemUtils;
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

        DefaultCryptoKeyReaderBuilder auxBuilder =
                DefaultCryptoKeyReader.builder().defaultPublicKey("file:" + ecdsaPublicKey)
                .defaultPrivateKey("file:" + ecdsaPrivateKey)
                .publicKey("key2", "file:invalid").privateKey("key2", "file:invalid").publicKey("key3", "data:invalid")
                .privateKey("key3", "data:invalid");
        DefaultCryptoKeyReader keyReader2;
        // windows use \r\n instead of \n, so the base64 would be different
        if (SystemUtils.IS_OS_WINDOWS) {
            keyReader2 = auxBuilder
                    .publicKey("key1",
                            "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0NCk1JSUJJa"
                                    + "kFOQmdrcWhraUc5dzBCQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBenpUVHpzU3NaRllsV3liWnJNTncN"
                                    + "CmFEamdxaW5JTm81eU5rSHVSQlBnMnI1NkJFYUhvVTV5K2NjRGh5eEJHQ0tQWmtUY1Fhc3ZZZ1dKM3N"
                                    + "IUktBbE4NCmRaTkc4R3QzazJTcmZEcnJ0ajFLTDNHNk5XUkE4VHF5Umt4eGw1dnBBTWM2OVVqWDlIUH"
                                    + "dTemxtckM3WlhtMWUNClN3WVRWNys3cXMvNjlKTEJuck1KY3NsK0l5WFVhaGhSbjh3MkZrczlKV3JpT"
                                    + "ktpMVBTZ0NQak1qZ0tCRjd5YUUNCkFUSjBHTVNNYzhGdlhXd0ZKc1dCV1FrVXdncWxFeFIxTURpVlZC"
                                    + "dHc5UXRKQjI5SU5pME5EczJQZWI2MUR0NDkNCk5abE4va2xKQ1hJVXRCU0lxZzlvK2lSS1Z3WExIbkl"
                                    + "NMFdIVm5tUm4yTUswbmYwMy9Ed0NJVm5iNWVsVG9aNzINCjh3SURBUUFCDQotLS0tLUVORCBQVUJMSU"
                                    + "MgS0VZLS0tLS0NCg==") // crypto_rsa_public.key
                    .privateKey("key1",
                            "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQ0K"
                                    + "TUlJRXBBSUJBQUtDQVFFQXp6VFR6c1NzWkZZbFd5YlpyTU53YURqZ3FpbklObzV5TmtIdVJCUGcycjU"
                                    + "2QkVhSA0Kb1U1eStjY0RoeXhCR0NLUFprVGNRYXN2WWdXSjNzSFJLQWxOZFpORzhHdDNrMlNyZkRycn"
                                    + "RqMUtMM0c2TldSQQ0KOFRxeVJreHhsNXZwQU1jNjlValg5SFB3U3psbXJDN1pYbTFlU3dZVFY3Kzdxc"
                                    + "y82OUpMQm5yTUpjc2wrSXlYVQ0KYWhoUm44dzJGa3M5SldyaU5LaTFQU2dDUGpNamdLQkY3eWFFQVRK"
                                    + "MEdNU01jOEZ2WFd3RkpzV0JXUWtVd2dxbA0KRXhSMU1EaVZWQnR3OVF0SkIyOUlOaTBORHMyUGViNjF"
                                    + "EdDQ5TlpsTi9rbEpDWElVdEJTSXFnOW8raVJLVndYTA0KSG5JTTBXSFZubVJuMk1LMG5mMDMvRHdDSV"
                                    + "ZuYjVlbFRvWjcyOHdJREFRQUJBb0lCQUhXRnZmaVJua0dPaHNPTA0KWm56Um9Nak1NY2p4eDhnQnhRK"
                                    + "zNGMS92Y21JL0ZNK2wvVGxsV0ZzSklKd2pZb3hBMWRxb2hkQ05PbU83Um5aYw0Kc2JlbWh4Ti94QVdL"
                                    + "dnBpUHlaKzlmNEd1ZzR3alVmMEVicjBqa0lmRXhjeTZ0azRsc2UvN0w5bExoT2YxbDZGag0KTlJDVXN"
                                    + "aMlZ4WlRJZjdXakh2Qm02SUNOaFhkZmdjL1RPWC9INEJCTXh5UWtrbXZTN3lRSFBtbmVrVnBDandYaQ"
                                    + "0KUmdkUE9wQlNIVUFzdUxjM2NkTzdEelNsWEJ5K1IzVUI1YkM5N2VmU1R3eG1NZGNHVU5RaDE3Q3V3K"
                                    + "291Mk9MSg0KbC9XeU1CSmdLUDB6cDhNSTJZQ1AwdG9FMVZWMEZXaXNpTlVkeXcybW1kc0tCUEN0Wlc0"
                                    + "SmYvYXZSTGpDcHk4Mw0KZ3llSGk0a0NnWUVBN2ZhYzh1L1dvVWNSeGZ3YmV4eFZOSWI1anBWZ1EyMlh"
                                    + "YVXZjT0JBMzE0NUhGSDRrRDlRcw0KT2xPTTQ4aUVYMUdxK0ZPYitUa1ZhM3llRWJ5RUhWbU54bTdacU"
                                    + "RFbEdsUG5ISGdXSmZWbzRsdGVta05ROGNRSQ0KQ2lEaFVIN0Q5aUdkNFRyTHErdThKWS9veTBkcEp5Y"
                                    + "UovR3NOUHdqVnpNaUE5a0R1STJLS1JwY0NnWUVBM3VscA0Kc1p5ODJFaWJUK1lZTnNoZkF5elBjUWMr"
                                    + "Q1ZkY3BTM3lFVU1jTXJyR1QySUhiQWhsZHpBV3ZuaTdQRjFDdllvQg0KZW9QU0dpRUdNQXdBZmFVSTR"
                                    + "wc1p2QVRaVGYrR1dLaXpsSDgyMGx3NHRZMk5XMnRZRndEY1o2RVBLZHE5WkNPeg0KTGZ5dzJOaExxaT"
                                    + "JGcEZ5QXBjWGxBOTJVVUkwRmNYQ0N0QUtKMmdVQ2dZRUFzSTVtZXJWS2lOVERNaU5RZkhKVQ0KSWFuM"
                                    + "3BUdmRkWW50WVhKMGpVQXpQb0s0NkZLRERSOStSVFJTZDNzQ0Evc0RJRVpnbG5RdEdWZ1hxODgwTXRh"
                                    + "TQ0KSTJ1Qm9KSCtGbCtrUFBJNGRLZDF6MlM5ZHpWMDdEeG5QcVNRcC9tMkNYdDl1V3UzUy9LVzRVTzZ"
                                    + "GSUVDV3VMMA0KUkUxbFFaeWJqTnBESFBLbC9ha1NNVGNDZ1lCcUM4MFdqQ1JqR0plYXVUSkh6YWMxMF"
                                    + "htV29nVm5XRUpnNnF6QQ0KZlpiS280UjRlNEJnYXRZcWo1d2lYVGxtREZBVjc3T29YMUh5MEVjclVHc"
                                    + "GpXOElRWEEwd0gzWnAzdWhCQVhEOQ0KY2svWVg3c3kwL2F0eVRHTk1BR3E0enBkaFF5WXVVc2kwNVlt"
                                    + "Y3kvNzg5QWlVMGQ0bGQ3UHFmaEhJZSsyK2ZtVQ0KUGFqcktRS0JnUUNqcGowWStFL0hpMFNKVUtOVlB"
                                    + "sM3UrU04xbExjUjEvZ01qSmJzWUZHdWExTXNMWEJOWFRTbA0KUWlZSGlhZFQ3QmhQRWtGZFc3dStiRn"
                                    + "dzMmFkbVcxOUJvVWIrd2d0WlQvdDduVHlvUzRMYWc0dnlhek5QWnpkUQ0KeDZYUHJ3WmltZDBYRERpd"
                                    + "EdMamNMZjk5MUVGc1hTcVB6bkREZlh0SjMxK29VNk9ibkhTSXRBPT0NCi0tLS0tRU5EIFJTQSBQUklW"
                                    + "QVRFIEtFWS0tLS0tDQo=")
                    .build();
        } else {
            keyReader2 = auxBuilder
                    .publicKey("key1",
                            "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlJQklqQ"
                                    + "U5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF6elRUenNTc1pGWWxXeWJack1Odwph"
                                    + "RGpncWluSU5vNXlOa0h1UkJQZzJyNTZCRWFIb1U1eStjY0RoeXhCR0NLUFprVGNRYXN2WWdXSjNzSFJ"
                                    + "LQWxOCmRaTkc4R3QzazJTcmZEcnJ0ajFLTDNHNk5XUkE4VHF5Umt4eGw1dnBBTWM2OVVqWDlIUHdTem"
                                    + "xtckM3WlhtMWUKU3dZVFY3Kzdxcy82OUpMQm5yTUpjc2wrSXlYVWFoaFJuOHcyRmtzOUpXcmlOS2kxU"
                                    + "FNnQ1BqTWpnS0JGN3lhRQpBVEowR01TTWM4RnZYV3dGSnNXQldRa1V3Z3FsRXhSMU1EaVZWQnR3OVF0"
                                    + "SkIyOUlOaTBORHMyUGViNjFEdDQ5Ck5abE4va2xKQ1hJVXRCU0lxZzlvK2lSS1Z3WExIbklNMFdIVm5"
                                    + "tUm4yTUswbmYwMy9Ed0NJVm5iNWVsVG9aNzIKOHdJREFRQUIKLS0tLS1FTkQgUFVCTElDIEtFWS0tLS"
                                    + "0tCg==") // crypto_rsa_public.key
                    .privateKey("key1",
                            "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpN"
                                    + "SUlFcEFJQkFBS0NBUUVBenpUVHpzU3NaRllsV3liWnJNTndhRGpncWluSU5vNXlOa0h1UkJQZzJyNTZ"
                                    + "CRWFICm9VNXkrY2NEaHl4QkdDS1Baa1RjUWFzdllnV0ozc0hSS0FsTmRaTkc4R3QzazJTcmZEcnJ0aj"
                                    + "FLTDNHNk5XUkEKOFRxeVJreHhsNXZwQU1jNjlValg5SFB3U3psbXJDN1pYbTFlU3dZVFY3Kzdxcy82O"
                                    + "UpMQm5yTUpjc2wrSXlYVQphaGhSbjh3MkZrczlKV3JpTktpMVBTZ0NQak1qZ0tCRjd5YUVBVEowR01T"
                                    + "TWM4RnZYV3dGSnNXQldRa1V3Z3FsCkV4UjFNRGlWVkJ0dzlRdEpCMjlJTmkwTkRzMlBlYjYxRHQ0OU5"
                                    + "abE4va2xKQ1hJVXRCU0lxZzlvK2lSS1Z3WEwKSG5JTTBXSFZubVJuMk1LMG5mMDMvRHdDSVZuYjVlbF"
                                    + "RvWjcyOHdJREFRQUJBb0lCQUhXRnZmaVJua0dPaHNPTApabnpSb01qTU1janh4OGdCeFErM0YxL3Zjb"
                                    + "UkvRk0rbC9UbGxXRnNKSUp3allveEExZHFvaGRDTk9tTzdSblpjCnNiZW1oeE4veEFXS3ZwaVB5Wis5"
                                    + "ZjRHdWc0d2pVZjBFYnIwamtJZkV4Y3k2dGs0bHNlLzdMOWxMaE9mMWw2RmoKTlJDVXNaMlZ4WlRJZjd"
                                    + "Xakh2Qm02SUNOaFhkZmdjL1RPWC9INEJCTXh5UWtrbXZTN3lRSFBtbmVrVnBDandYaQpSZ2RQT3BCU0"
                                    + "hVQXN1TGMzY2RPN0R6U2xYQnkrUjNVQjViQzk3ZWZTVHd4bU1kY0dVTlFoMTdDdXcrb3UyT0xKCmwvV"
                                    + "3lNQkpnS1AwenA4TUkyWUNQMHRvRTFWVjBGV2lzaU5VZHl3Mm1tZHNLQlBDdFpXNEpmL2F2UkxqQ3B5"
                                    + "ODMKZ3llSGk0a0NnWUVBN2ZhYzh1L1dvVWNSeGZ3YmV4eFZOSWI1anBWZ1EyMlhYVXZjT0JBMzE0NUh"
                                    + "GSDRrRDlRcwpPbE9NNDhpRVgxR3ErRk9iK1RrVmEzeWVFYnlFSFZtTnhtN1pxREVsR2xQbkhIZ1dKZl"
                                    + "ZvNGx0ZW1rTlE4Y1FJCkNpRGhVSDdEOWlHZDRUckxxK3U4Slkvb3kwZHBKeWFKL0dzTlB3alZ6TWlBO"
                                    + "WtEdUkyS0tScGNDZ1lFQTN1bHAKc1p5ODJFaWJUK1lZTnNoZkF5elBjUWMrQ1ZkY3BTM3lFVU1jTXJy"
                                    + "R1QySUhiQWhsZHpBV3ZuaTdQRjFDdllvQgplb1BTR2lFR01Bd0FmYVVJNHBzWnZBVFpUZitHV0tpemx"
                                    + "IODIwbHc0dFkyTlcydFlGd0RjWjZFUEtkcTlaQ096CkxmeXcyTmhMcWkyRnBGeUFwY1hsQTkyVVVJME"
                                    + "ZjWENDdEFLSjJnVUNnWUVBc0k1bWVyVktpTlRETWlOUWZISlUKSWFuM3BUdmRkWW50WVhKMGpVQXpQb"
                                    + "0s0NkZLRERSOStSVFJTZDNzQ0Evc0RJRVpnbG5RdEdWZ1hxODgwTXRhTQpJMnVCb0pIK0ZsK2tQUEk0"
                                    + "ZEtkMXoyUzlkelYwN0R4blBxU1FwL20yQ1h0OXVXdTNTL0tXNFVPNkZJRUNXdUwwClJFMWxRWnliak5"
                                    + "wREhQS2wvYWtTTVRjQ2dZQnFDODBXakNSakdKZWF1VEpIemFjMTBYbVdvZ1ZuV0VKZzZxekEKZlpiS2"
                                    + "80UjRlNEJnYXRZcWo1d2lYVGxtREZBVjc3T29YMUh5MEVjclVHcGpXOElRWEEwd0gzWnAzdWhCQVhEO"
                                    + "Qpjay9ZWDdzeTAvYXR5VEdOTUFHcTR6cGRoUXlZdVVzaTA1WW1jeS83ODlBaVUwZDRsZDdQcWZoSEll"
                                    + "KzIrZm1VClBhanJLUUtCZ1FDanBqMFkrRS9IaTBTSlVLTlZQbDN1K1NOMWxMY1IxL2dNakpic1lGR3V"
                                    + "hMU1zTFhCTlhUU2wKUWlZSGlhZFQ3QmhQRWtGZFc3dStiRndzMmFkbVcxOUJvVWIrd2d0WlQvdDduVH"
                                    + "lvUzRMYWc0dnlhek5QWnpkUQp4NlhQcndaaW1kMFhERGl0R0xqY0xmOTkxRUZzWFNxUHpuRERmWHRKM"
                                    + "zErb1U2T2JuSFNJdEE9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=")
                    .build();
        }

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

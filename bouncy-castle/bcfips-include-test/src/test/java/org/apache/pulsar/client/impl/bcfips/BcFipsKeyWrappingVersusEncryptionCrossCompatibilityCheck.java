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
package org.apache.pulsar.client.impl.bcfips;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;
import org.apache.commons.codec.DecoderException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.crypto.BcVersionSpecificCryptoUtility;
import org.apache.pulsar.client.impl.crypto.EncKeyReader;
import org.apache.pulsar.client.impl.crypto.WrappingVersusEncryptionCrossCompatibilityTestBase;
import org.apache.pulsar.client.impl.crypto.bcfips.BCFipsSpecificUtility;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BcFipsKeyWrappingVersusEncryptionCrossCompatibilityCheck
        extends WrappingVersusEncryptionCrossCompatibilityTestBase {

    @BeforeClass
    public static void setUp() {
        //testing in approved mode
        System.setProperty("org.bouncycastle.fips.approved_only", "true");
    }

    @DataProvider
    @Override
    public Object[][] badEncryptionInputs()
            throws NoSuchAlgorithmException, IOException, InvalidKeySpecException, NoSuchProviderException,
            DecoderException {
        return new Object[][]{
                {EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-256.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-256.pem", ImmutableMap.of()),
                        loadAESKEy("aes128bit"),
                        allOf(
                                instanceOf(PulsarClientException.CryptoException.class),
                                hasProperty("message", containsString("Unable to wrap key: input data too long"))
                        )
                },
                {EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-256.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-256.pem", ImmutableMap.of()),
                        loadAESKEy("aes256bit"),
                        allOf(
                                instanceOf(PulsarClientException.CryptoException.class),
                                hasProperty("message", containsString("Unable to wrap key: input data too long"))
                        )
                }
        };
    }

    @Test
    public void testFipsLoadedInApprovedMode() {
        BcVersionSpecificCryptoUtility bcFipsSpecificUtility = BcVersionSpecificCryptoUtility.INSTANCE;

        assertTrue(CryptoServicesRegistrar.isInApprovedOnlyMode());
        assertTrue(bcFipsSpecificUtility instanceof BCFipsSpecificUtility);
    }
}

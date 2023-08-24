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
package org.apache.pulsar.client.impl.crypto;

import static org.apache.pulsar.client.impl.crypto.BcVersionSpecificCryptoUtilityFactory.createNewInstance;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Optional;
import javax.crypto.SecretKey;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Facade to various crypto functions implemented by BouncyCastle FIPS and Non-fips module.
 */
public interface BcVersionSpecificCryptoUtility {

    BcVersionSpecificCryptoUtility INSTANCE = createNewInstance();

    String RSA = "RSA";

    // Ideally the transformation should also be part of the message property. This will prevent client
    // from assuming hardcoded value. However, it will increase the size of the message even further.
    String RSA_TRANS = "RSA/NONE/OAEPWithSHA1AndMGF1Padding";


    byte[] encryptDataKey(String logCtx, String keyName, PublicKey publicKey, SecretKey dataKey)
            throws PulsarClientException.CryptoException;

    Optional<SecretKey> deCryptDataKey(String datakeyAlgorithm, String logCtx, String keyName, PrivateKey privateKey,
                                       byte[] encryptedDataKey);

    PublicKey loadPublicKey(byte[] keyBytes);

    PrivateKey loadPrivateKey(byte[] keyBytes);
}



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

import java.lang.reflect.InvocationTargetException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Optional;
import javax.crypto.SecretKey;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.SecurityUtility;

public interface BcVersionSpecificUtility {
    static BcVersionSpecificUtility getInstance() {
        String providerName = SecurityUtility.getProvider().getName();
        try {
            switch (providerName) {
                case SecurityUtility.BC:
                    return (BcVersionSpecificUtility) BcVersionSpecificUtility.class.getClassLoader()
                            .loadClass("org.apache.pulsar.client.impl.crypto.bc.BCNonFipsSpecificUtility").getConstructor()
                            .newInstance();
                case SecurityUtility.BC_FIPS:
                    return (BcVersionSpecificUtility) BcVersionSpecificUtility.class.getClassLoader()
                            .loadClass("org.apache.pulsar.client.impl.bcfips.BCFipsSpecificUtility")
                            .getConstructor().newInstance();
                default:
                    throw new IllegalStateException(
                            "Provider not handled for BC Specific delegate creation: " + providerName);
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException
                 | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    byte[] encryptDataKey(String logCtx, String keyName, PublicKey publicKey, SecretKey dataKey)
            throws PulsarClientException.CryptoException;

    Optional<SecretKey> deCryptDataKey(String datakeyAlgorithm, String logCtx, String keyName, PrivateKey privateKey,
                                       byte[] encryotedDataKey);

    PublicKey loadPublicKey(byte[] keyBytes);

    PrivateKey loadPrivateKey(byte[] keyBytes);
}



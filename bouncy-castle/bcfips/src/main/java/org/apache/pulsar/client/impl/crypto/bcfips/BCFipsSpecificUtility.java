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
package org.apache.pulsar.client.impl.crypto.bcfips;

import static javax.crypto.Cipher.SECRET_KEY;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Optional;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.crypto.BcVersionSpecificCryptoUtility;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.ECNamedCurveTable;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

@Slf4j
public class BCFipsSpecificUtility implements BcVersionSpecificCryptoUtility {

    private static final String providerName = BouncyCastleFipsProvider.PROVIDER_NAME;

    private static final String EC = "EC";


    @Override
    public byte[] encryptDataKey(String logCtx, String keyName, PublicKey pubKey, SecretKey dataKey)
            throws PulsarClientException.CryptoException {
        try {
            Cipher dataKeyCipher = null;
            if (RSA.equals(pubKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, providerName);
            } else {
                String msg = logCtx + "Unsupported key type " + pubKey.getAlgorithm() + " for key " + keyName;
                log.error(msg);
                throw new PulsarClientException.CryptoException(msg);
            }
            dataKeyCipher.init(Cipher.WRAP_MODE, pubKey);
            return dataKeyCipher.wrap(dataKey);
        } catch (IllegalBlockSizeException | NoSuchAlgorithmException | NoSuchProviderException
                 | NoSuchPaddingException | InvalidKeyException e) {
            log.error("{} Failed to encrypt data key {}. {}", logCtx, keyName, e.getMessage(), e);
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
    }

    @Override
    public Optional<SecretKey> deCryptDataKey(String datakeyAlgorithm, String logCtx, String keyName,
                                              PrivateKey privateKey, byte[] encryptedDataKey) {
        try {
            Cipher dataKeyCipher = null;
            if (RSA.equals(privateKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, providerName);
            } else {
                log.error("Unsupported key type {} for key {}.", privateKey.getAlgorithm(), keyName);
                return Optional.empty();
            }
            dataKeyCipher.init(Cipher.UNWRAP_MODE, privateKey);
            return Optional.of(
                    new SecretKeySpec(
                            dataKeyCipher.unwrap(encryptedDataKey, datakeyAlgorithm, SECRET_KEY).getEncoded(),
                            datakeyAlgorithm));
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException | InvalidKeyException e) {
            log.error("{} Failed to decrypt data key {} to decrypt messages {}", logCtx, keyName, e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public PublicKey loadPublicKey(byte[] keyBytes) {

        Reader keyReader = new StringReader(new String(keyBytes));
        PublicKey publicKey = null;
        try (PEMParser pemReader = new PEMParser(keyReader)) {
            Object pemObj = pemReader.readObject();
            JcaPEMKeyConverter pemConverter = new JcaPEMKeyConverter();
            SubjectPublicKeyInfo keyInfo = null;
            X9ECParameters ecParam = null;

            if (pemObj instanceof ASN1ObjectIdentifier) {

                // make sure this is EC Parameter we're handling. In which case
                // we'll store it and read the next object which should be our
                // EC Public Key

                ASN1ObjectIdentifier ecOID = (ASN1ObjectIdentifier) pemObj;
                ecParam = ECNamedCurveTable.getByOID(ecOID);
                if (ecParam == null) {
                    throw new PEMException("Unable to find EC Parameter for the given curve oid: "
                            + ((ASN1ObjectIdentifier) pemObj).getId());
                }

                pemObj = pemReader.readObject();
            } else if (pemObj instanceof X9ECParameters) {
                ecParam = (X9ECParameters) pemObj;
                pemObj = pemReader.readObject();
            }

            if (pemObj instanceof X509CertificateHolder) {
                keyInfo = ((X509CertificateHolder) pemObj).getSubjectPublicKeyInfo();
            } else {
                keyInfo = (SubjectPublicKeyInfo) pemObj;
            }
            publicKey = pemConverter.getPublicKey(keyInfo);

            if (ecParam != null && EC.equals(publicKey.getAlgorithm())) {
                throw new IllegalArgumentException("ECDSA keys are not supported in FIPS mode to wrap data key!");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return publicKey;
    }

    @Override
    public PrivateKey loadPrivateKey(byte[] keyBytes) {

        Reader keyReader = new StringReader(new String(keyBytes));
        PrivateKey privateKey = null;
        try (PEMParser pemReader = new PEMParser(keyReader)) {
            X9ECParameters ecParam = null;

            Object pemObj = pemReader.readObject();

            if (pemObj instanceof ASN1ObjectIdentifier) {

                // make sure this is EC Parameter we're handling. In which case
                // we'll store it and read the next object which should be our
                // EC Private Key

                ASN1ObjectIdentifier ecOID = (ASN1ObjectIdentifier) pemObj;
                ecParam = ECNamedCurveTable.getByOID(ecOID);
                if (ecParam == null) {
                    throw new PEMException("Unable to find EC Parameter for the given curve oid: " + ecOID.getId());
                }

                pemObj = pemReader.readObject();

            } else if (pemObj instanceof X9ECParameters) {

                ecParam = (X9ECParameters) pemObj;
                pemObj = pemReader.readObject();
            }

            if (pemObj instanceof PEMKeyPair) {

                PrivateKeyInfo pKeyInfo = ((PEMKeyPair) pemObj).getPrivateKeyInfo();
                JcaPEMKeyConverter pemConverter = new JcaPEMKeyConverter();
                privateKey = pemConverter.getPrivateKey(pKeyInfo);

            }

            // if our private key is EC type and we have parameters specified
            // then we need to set it accordingly

            if (ecParam != null && (EC.equals(privateKey.getAlgorithm()))) {
                throw new IllegalArgumentException("ECDSA keys are not supported in FIPS mode to wrap data key!");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return privateKey;
    }

}

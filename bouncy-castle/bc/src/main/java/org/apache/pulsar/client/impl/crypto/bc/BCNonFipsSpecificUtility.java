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
package org.apache.pulsar.client.impl.crypto.bc;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.util.Optional;
import javax.crypto.BadPaddingException;
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
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPrivateKey;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.jce.spec.ECPrivateKeySpec;
import org.bouncycastle.jce.spec.ECPublicKeySpec;
import org.bouncycastle.jce.spec.IESParameterSpec;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

@Slf4j
public class BCNonFipsSpecificUtility implements BcVersionSpecificCryptoUtility {

    private static final String providerName = BouncyCastleProvider.PROVIDER_NAME;

    private static final String ECDSA = "ECDSA";

    //TODO: ECIES does not support key wrapping, so cannot change the code to use wrap API, worth considering using
    //      something else e.g. ETSIKEMwithSHA256, but that would break old consumers so the wrapping method should be
    //      passed on in message header for example
    private static final String ECIES = "ECIES";

    @Override
    public byte[] encryptDataKey(String logCtx, String keyName, PublicKey pubKey, SecretKey dataKey)
            throws PulsarClientException.CryptoException {
        Cipher dataKeyCipher = null;
        try {

            AlgorithmParameterSpec params = null;
            // Encrypt data key using public key
            if (RSA.equals(pubKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, providerName);
            } else {
                if (ECDSA.equals(pubKey.getAlgorithm())) {
                    dataKeyCipher = Cipher.getInstance(ECIES, providerName);
                    params = createIESParameterSpec();
                } else {
                    String msg = logCtx + "Unsupported key type " + pubKey.getAlgorithm() + " for key " + keyName;
                    log.error(msg);
                    throw new PulsarClientException.CryptoException(msg);
                }
            }
            //TODO this is really a WRAP - but has to be tested for backwards compatibility if we can just change it to
            // WRAP + ECIES is not supporting wrap operation
            if (params != null) {
                dataKeyCipher.init(Cipher.ENCRYPT_MODE, pubKey, params);
            } else {
                dataKeyCipher.init(Cipher.ENCRYPT_MODE, pubKey);
            }
            return dataKeyCipher.doFinal(dataKey.getEncoded());

        } catch (IllegalBlockSizeException | NoSuchAlgorithmException | NoSuchProviderException
                 | NoSuchPaddingException | InvalidKeyException | BadPaddingException
                 | InvalidAlgorithmParameterException e) {
            log.error("{} Failed to encrypt data key {}. {}", logCtx, keyName, e.getMessage());
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
    }

    @Override
    public Optional<SecretKey> deCryptDataKey(String datakeyAlgorithm, String logCtx, String keyName,
                                              PrivateKey privateKey, byte[] encryptedDataKey) {
        Cipher dataKeyCipher = null;
        byte[] dataKeyValue = null;
        try {

            AlgorithmParameterSpec params = null;
            // Decrypt data key using private key
            if (RSA.equals(privateKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, providerName);
            } else if (ECDSA.equals(privateKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(ECIES, providerName);
                params = createIESParameterSpec();
            } else {
                log.error("Unsupported key type {} for key {}.", privateKey.getAlgorithm(), keyName);
                return Optional.empty();
            }
            //TODO this is really an UNWRAP - but has to be tested for backwards compatibility if we can just change it
            // to UNWRAP + ECIES is not supporting wrap operation
            if (params != null) {
                dataKeyCipher.init(Cipher.DECRYPT_MODE, privateKey, params);
            } else {
                dataKeyCipher.init(Cipher.DECRYPT_MODE, privateKey);
            }
            dataKeyValue = dataKeyCipher.doFinal(encryptedDataKey);
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException | InvalidKeyException
                 | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException e) {
            log.error("{} Failed to decrypt data key {} to decrypt messages {}", logCtx, keyName, e.getMessage());
            return Optional.empty();
        }
        return Optional.of(new SecretKeySpec(dataKeyValue, datakeyAlgorithm));
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

            if (ecParam != null && (ECDSA.equals(publicKey.getAlgorithm()))) {
                ECParameterSpec ecSpec = new ECParameterSpec(ecParam.getCurve(), ecParam.getG(), ecParam.getN(),
                        ecParam.getH(), ecParam.getSeed());
                KeyFactory keyFactory = KeyFactory.getInstance(ECDSA, providerName);
                ECPublicKeySpec keySpec = new ECPublicKeySpec(((BCECPublicKey) publicKey).getQ(), ecSpec);
                publicKey = keyFactory.generatePublic(keySpec);
            }
        } catch (IOException | NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException e) {
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

            if (ecParam != null && (ECDSA.equals(privateKey.getAlgorithm()))) {
                ECParameterSpec ecSpec = new ECParameterSpec(ecParam.getCurve(), ecParam.getG(), ecParam.getN(),
                        ecParam.getH(), ecParam.getSeed());
                KeyFactory keyFactory = KeyFactory.getInstance(ECDSA, providerName);
                ECPrivateKeySpec keySpec = new ECPrivateKeySpec(((BCECPrivateKey) privateKey).getS(), ecSpec);
                privateKey = keyFactory.generatePrivate(keySpec);
            }

        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException | NoSuchProviderException e) {
            throw new RuntimeException(e);
        }
        return privateKey;
    }

    // required since Bouncycastle 1.72 when using ECIES, it is required to pass in an IESParameterSpec
    private static IESParameterSpec createIESParameterSpec() {
        // the IESParameterSpec to use was discovered by debugging BouncyCastle 1.69 and running the
        // test org.apache.pulsar.client.api.SimpleProducerConsumerTest#testCryptoWithChunking
        return new IESParameterSpec(null, null, 128);
    }

}

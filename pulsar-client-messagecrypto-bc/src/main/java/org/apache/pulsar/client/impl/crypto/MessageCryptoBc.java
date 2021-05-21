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
package org.apache.pulsar.client.impl.crypto;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
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
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

@Slf4j
public class MessageCryptoBc implements MessageCrypto<MessageMetadata, MessageMetadata> {

    private static final String ECDSA = "ECDSA";
    private static final String RSA = "RSA";
    private static final String ECIES = "ECIES";

    // Ideally the transformation should also be part of the message property. This will prevent client
    // from assuming hardcoded value. However, it will increase the size of the message even further.
    private static final String RSA_TRANS = "RSA/NONE/OAEPWithSHA1AndMGF1Padding";
    private static final String AESGCM = "AES/GCM/NoPadding";

    private static KeyGenerator keyGenerator;
    private static final int tagLen = 16 * 8;
    private byte[] iv = new byte[IV_LEN];
    private Cipher cipher;
    MessageDigest digest;
    private String logCtx;

    // Data key which is used to encrypt message
    private SecretKey dataKey;
    private LoadingCache<ByteBuffer, SecretKey> dataKeyCache;

    // Map of key name and encrypted gcm key, metadata pair which is sent with encrypted message
    private ConcurrentHashMap<String, EncryptionKeyInfo> encryptedDataKeyMap;

    static final SecureRandom secureRandom;
    static {
        SecureRandom rand = null;
        try {
            rand = SecureRandom.getInstance("NativePRNGNonBlocking");
        } catch (NoSuchAlgorithmException nsa) {
            rand = new SecureRandom();
        }

        secureRandom = rand;

        // Initial seed
        secureRandom.nextBytes(new byte[IV_LEN]);

        // Add provider only if it's not in the JVM
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    public MessageCryptoBc(String logCtx, boolean keyGenNeeded) {

        this.logCtx = logCtx;
        encryptedDataKeyMap = new ConcurrentHashMap<String, EncryptionKeyInfo>();
        dataKeyCache = CacheBuilder.newBuilder().expireAfterAccess(4, TimeUnit.HOURS)
                .build(new CacheLoader<ByteBuffer, SecretKey>() {

                    @Override
                    public SecretKey load(ByteBuffer key) {
                        return null;
                    }

                });

        try {

            cipher = Cipher.getInstance(AESGCM, BouncyCastleProvider.PROVIDER_NAME);
            // If keygen is not needed(e.g: consumer), data key will be decrypted from the message
            if (!keyGenNeeded) {

                digest = MessageDigest.getInstance("MD5");

                dataKey = null;
                return;
            }
            keyGenerator = KeyGenerator.getInstance("AES");
            int aesKeyLength = Cipher.getMaxAllowedKeyLength("AES");
            if (aesKeyLength <= 128) {
                log.warn(
                        "{} AES Cryptographic strength is limited to {} bits. Consider installing JCE Unlimited Strength Jurisdiction Policy Files.",
                        logCtx, aesKeyLength);
                keyGenerator.init(aesKeyLength, secureRandom);
            } else {
                keyGenerator.init(256, secureRandom);
            }

        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {

            cipher = null;
            log.error("{} MessageCrypto initialization Failed {}", logCtx, e.getMessage());

        }

        // Generate data key to encrypt messages
        dataKey = keyGenerator.generateKey();

        iv = new byte[IV_LEN];
    }

    private PublicKey loadPublicKey(byte[] keyBytes) throws Exception {

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

            if (ecParam != null && ECDSA.equals(publicKey.getAlgorithm())) {
                ECParameterSpec ecSpec = new ECParameterSpec(ecParam.getCurve(), ecParam.getG(), ecParam.getN(),
                        ecParam.getH(), ecParam.getSeed());
                KeyFactory keyFactory = KeyFactory.getInstance(ECDSA, BouncyCastleProvider.PROVIDER_NAME);
                ECPublicKeySpec keySpec = new ECPublicKeySpec(((BCECPublicKey) publicKey).getQ(), ecSpec);
                publicKey = keyFactory.generatePublic(keySpec);
            }
        } catch (IOException | NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException e) {
            throw new Exception(e);
        }
        return publicKey;
    }

    private PrivateKey loadPrivateKey(byte[] keyBytes) throws Exception {

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

            if (ecParam != null && ECDSA.equals(privateKey.getAlgorithm())) {
                ECParameterSpec ecSpec = new ECParameterSpec(ecParam.getCurve(), ecParam.getG(), ecParam.getN(),
                        ecParam.getH(), ecParam.getSeed());
                KeyFactory keyFactory = KeyFactory.getInstance(ECDSA, BouncyCastleProvider.PROVIDER_NAME);
                ECPrivateKeySpec keySpec = new ECPrivateKeySpec(((BCECPrivateKey) privateKey).getS(), ecSpec);
                privateKey = keyFactory.generatePrivate(keySpec);
            }

        } catch (IOException e) {
            throw new Exception(e);
        }
        return privateKey;
    }

    /*
     * Encrypt data key using the public key(s) in the argument. <p> If more than one key name is specified, data key is
     * encrypted using each of those keys. If the public key is expired or changed, application is responsible to remove
     * the old key and add the new key <p>
     *
     * @param keyNames List of public keys to encrypt data key
     *
     * @param keyReader Implementation to read the key values
     *
     */
    @Override
    public synchronized void addPublicKeyCipher(Set<String> keyNames, CryptoKeyReader keyReader)
            throws CryptoException {

        // Generate data key
        dataKey = keyGenerator.generateKey();

        for (String key : keyNames) {
            addPublicKeyCipher(key, keyReader);
        }
    }

    private void addPublicKeyCipher(String keyName, CryptoKeyReader keyReader) throws CryptoException {

        if (keyName == null || keyReader == null) {
            throw new PulsarClientException.CryptoException("Keyname or KeyReader is null");
        }

        // Read the public key and its info using callback
        EncryptionKeyInfo keyInfo = keyReader.getPublicKey(keyName, null);

        PublicKey pubKey;

        try {
            pubKey = loadPublicKey(keyInfo.getKey());
        } catch (Exception e) {
            String msg = logCtx + "Failed to load public key " + keyName + ". " + e.getMessage();
            log.error(msg);
            throw new PulsarClientException.CryptoException(msg);
        }

        Cipher dataKeyCipher = null;
        byte[] encryptedKey;

        try {

            // Encrypt data key using public key
            if (RSA.equals(pubKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, BouncyCastleProvider.PROVIDER_NAME);
            } else if (ECDSA.equals(pubKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(ECIES, BouncyCastleProvider.PROVIDER_NAME);
            } else {
                String msg = logCtx + "Unsupported key type " + pubKey.getAlgorithm() + " for key " + keyName;
                log.error(msg);
                throw new PulsarClientException.CryptoException(msg);
            }
            dataKeyCipher.init(Cipher.ENCRYPT_MODE, pubKey);
            encryptedKey = dataKeyCipher.doFinal(dataKey.getEncoded());

        } catch (IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | NoSuchProviderException
                | NoSuchPaddingException | InvalidKeyException e) {
            log.error("{} Failed to encrypt data key {}. {}", logCtx, keyName, e.getMessage());
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
        EncryptionKeyInfo eki = new EncryptionKeyInfo(encryptedKey, keyInfo.getMetadata());
        encryptedDataKeyMap.put(keyName, eki);
    }

    /*
     * Remove a key <p> Remove the key identified by the keyName from the list of keys.<p>
     *
     * @param keyName Unique name to identify the key
     *
     * @return true if succeeded, false otherwise
     */
    @Override
    public boolean removeKeyCipher(String keyName) {

        if (keyName == null) {
            return false;
        }
        encryptedDataKeyMap.remove(keyName);
        return true;
    }

    /*
     * Encrypt the payload using the data key and update message metadata with the keyname & encrypted data key
     *
     * @param encKeys One or more public keys to encrypt data key
     *
     * @param msgMetadata Message Metadata
     *
     * @param payload Message which needs to be encrypted
     *
     * @return encryptedData if success
     */
    @Override
    public synchronized void encrypt(Set<String> encKeys, CryptoKeyReader keyReader,
                                        Supplier<MessageMetadata> messageMetadataBuilderSupplier,
                                     ByteBuffer payload, ByteBuffer outBuffer) throws PulsarClientException {

        MessageMetadata msgMetadata = messageMetadataBuilderSupplier.get();

        if (encKeys.isEmpty()) {
            outBuffer.put(payload);
            outBuffer.flip();
            return;
        }

        // Update message metadata with encrypted data key
        for (String keyName : encKeys) {
            if (encryptedDataKeyMap.get(keyName) == null) {
                // Attempt to load the key. This will allow us to load keys as soon as
                // a new key is added to producer config
                addPublicKeyCipher(keyName, keyReader);
            }
            EncryptionKeyInfo keyInfo = encryptedDataKeyMap.get(keyName);
            if (keyInfo != null) {
                if (keyInfo.getMetadata() != null && !keyInfo.getMetadata().isEmpty()) {
                    EncryptionKeys encKey = msgMetadata.addEncryptionKey()
                            .setKey(keyName)
                            .setValue(keyInfo.getKey());
                    keyInfo.getMetadata().forEach((key, value) -> {
                        encKey.addMetadata()
                                .setKey(key)
                                .setValue(value);
                    });
                } else {
                    msgMetadata.addEncryptionKey()
                            .setKey(keyName)
                            .setValue(keyInfo.getKey());
                }
            } else {
                // We should never reach here.
                log.error("{} Failed to find encrypted Data key for key {}.", logCtx, keyName);
            }

        }

        // Create gcm param
        // TODO: Replace random with counter and periodic refreshing based on timer/counter value
        secureRandom.nextBytes(iv);
        GCMParameterSpec gcmParam = new GCMParameterSpec(tagLen, iv);

        // Update message metadata with encryption param
        msgMetadata.setEncryptionParam(iv);

        try {
            // Encrypt the data
            cipher.init(Cipher.ENCRYPT_MODE, dataKey, gcmParam);

            int maxLength = cipher.getOutputSize(payload.remaining());
            if (outBuffer.remaining() < maxLength) {
                throw new IllegalArgumentException("Outbuffer has not enough space available");
            }

            int bytesStored = cipher.doFinal(payload, outBuffer);
            outBuffer.flip();
            outBuffer.limit(bytesStored);
        } catch (IllegalBlockSizeException | BadPaddingException | InvalidKeyException
                | InvalidAlgorithmParameterException | ShortBufferException e) {
            log.error("{} Failed to encrypt message. {}", logCtx, e);
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
    }

    private boolean decryptDataKey(String keyName, byte[] encryptedDataKey, List<KeyValue> encKeyMeta,
            CryptoKeyReader keyReader) {

        Map<String, String> keyMeta = new HashMap<String, String>();
        encKeyMeta.forEach(kv -> {
            keyMeta.put(kv.getKey(), kv.getValue());
        });

        // Read the private key info using callback
        EncryptionKeyInfo keyInfo = keyReader.getPrivateKey(keyName, keyMeta);

        // Convert key from byte to PrivateKey
        PrivateKey privateKey;
        try {
            privateKey = loadPrivateKey(keyInfo.getKey());
            if (privateKey == null) {
                log.error("{} Failed to load private key {}.", logCtx, keyName);
                return false;
            }
        } catch (Exception e) {
            log.error("{} Failed to decrypt data key {} to decrypt messages {}", logCtx, keyName, e.getMessage());
            return false;
        }

        // Decrypt data key to decrypt messages
        Cipher dataKeyCipher = null;
        byte[] dataKeyValue = null;
        byte[] keyDigest = null;

        try {

            // Decrypt data key using private key
            if (RSA.equals(privateKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, BouncyCastleProvider.PROVIDER_NAME);
            } else if (ECDSA.equals(privateKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(ECIES, BouncyCastleProvider.PROVIDER_NAME);
            } else {
                log.error("Unsupported key type {} for key {}.", privateKey.getAlgorithm(), keyName);
                return false;
            }
            dataKeyCipher.init(Cipher.DECRYPT_MODE, privateKey);
            dataKeyValue = dataKeyCipher.doFinal(encryptedDataKey);

            keyDigest = digest.digest(encryptedDataKey);

        } catch (IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | NoSuchProviderException
                | NoSuchPaddingException | InvalidKeyException e) {
            log.error("{} Failed to decrypt data key {} to decrypt messages {}", logCtx, keyName, e.getMessage());
            return false;
        }
        dataKey = new SecretKeySpec(dataKeyValue, "AES");
        dataKeyCache.put(ByteBuffer.wrap(keyDigest), dataKey);
        return true;
    }

    private boolean decryptData(SecretKey dataKeySecret, MessageMetadata msgMetadata,
                                ByteBuffer payload, ByteBuffer targetBuffer) {

        // unpack iv and encrypted data
        iv =  msgMetadata.getEncryptionParam();

        GCMParameterSpec gcmParams = new GCMParameterSpec(tagLen, iv);
        try {
            cipher.init(Cipher.DECRYPT_MODE, dataKeySecret, gcmParams);

            int maxLength = cipher.getOutputSize(payload.remaining());
            if (targetBuffer.remaining() < maxLength) {
                throw new IllegalArgumentException("Target buffer size is too small");
            }
            int decryptedSize = cipher.doFinal(payload, targetBuffer);
            targetBuffer.flip();
            targetBuffer.limit(decryptedSize);
            return true;

        } catch (InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException
                | BadPaddingException | ShortBufferException e) {
            log.error("{} Failed to decrypt message {}", logCtx, e.getMessage());
            return false;
        }
    }

    @Override
    public int getMaxOutputSize(int inputLen) {
        return inputLen + Math.max(inputLen, 512);
    }

    private boolean getKeyAndDecryptData(MessageMetadata msgMetadata, ByteBuffer payload, ByteBuffer targetBuffer) {
        List<EncryptionKeys> encKeys = msgMetadata.getEncryptionKeysList();

        // Go through all keys to retrieve data key from cache
        for (int i = 0; i < encKeys.size(); i++) {

            byte[] msgDataKey = encKeys.get(i).getValue();
            byte[] keyDigest = digest.digest(msgDataKey);
            SecretKey storedSecretKey = dataKeyCache.getIfPresent(ByteBuffer.wrap(keyDigest));
            if (storedSecretKey != null) {

                // Taking a small performance hit here if the hash collides. When it
                // retruns a different key, decryption fails. At this point, we would
                // call decryptDataKey to refresh the cache and come here again to decrypt.
                if (decryptData(storedSecretKey, msgMetadata, payload, targetBuffer)) {
                    // If decryption succeeded, we can already return
                    return true;
                }
            } else {
                // First time, entry won't be present in cache
                log.debug("{} Failed to decrypt data or data key is not in cache. Will attempt to refresh", logCtx);
            }

        }

        return false;
    }

    /*
     * Decrypt the payload using the data key. Keys used to encrypt data key can be retrieved from msgMetadata
     *
     * @param msgMetadata Message Metadata
     *
     * @param payload Message which needs to be decrypted
     *
     * @param keyReader KeyReader implementation to retrieve key value
     *
     * @return true if success, false otherwise
     */
    @Override
    public boolean decrypt(Supplier<MessageMetadata> messageMetadataSupplier,
                        ByteBuffer payload, ByteBuffer outBuffer, CryptoKeyReader keyReader) {

        MessageMetadata msgMetadata = messageMetadataSupplier.get();
        // If dataKey is present, attempt to decrypt using the existing key
        if (dataKey != null) {
            if (getKeyAndDecryptData(msgMetadata, payload, outBuffer)) {
                return true;
            }
        }

        // dataKey is null or decryption failed. Attempt to regenerate data key
        List<EncryptionKeys> encKeys = msgMetadata.getEncryptionKeysList();
        EncryptionKeys encKeyInfo = encKeys.stream().filter(kbv -> {

            byte[] encDataKey = kbv.getValue();
            List<KeyValue> encKeyMeta = kbv.getMetadatasList();
            return decryptDataKey(kbv.getKey(), encDataKey, encKeyMeta, keyReader);

        }).findFirst().orElse(null);

        if (encKeyInfo == null || dataKey == null) {
            // Unable to decrypt data key
            return false;
        }

        return getKeyAndDecryptData(msgMetadata, payload, outBuffer);

    }
}

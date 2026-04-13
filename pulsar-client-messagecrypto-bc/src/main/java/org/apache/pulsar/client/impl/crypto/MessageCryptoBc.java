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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
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
import lombok.CustomLog;
import lombok.Getter;
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
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.jce.spec.ECPrivateKeySpec;
import org.bouncycastle.jce.spec.ECPublicKeySpec;
import org.bouncycastle.jce.spec.IESParameterSpec;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

@CustomLog
public class MessageCryptoBc implements MessageCrypto<MessageMetadata, MessageMetadata> {
    public static final String ECDSA = "ECDSA";
    public static final String RSA = "RSA";
    public static final String ECIES = "ECIES";

    // Ideally the transformation should also be part of the message property. This will prevent client
    // from assuming hardcoded value. However, it will increase the size of the message even further.
    public static final String RSA_TRANS = "RSA/NONE/OAEPWithSHA1AndMGF1Padding";
    public static final String AESGCM = "AES/GCM/NoPadding";
    private static final String AESGCM_PROVIDER_NAME;

    private static final int tagLen = 16 * 8;
    private final String logCtx;

    // Data key which is used to encrypt message
    @Getter
    private volatile SecretKey encryptionKey;
    private final Cache<SecretKeyCacheKey, SecretKeySpec> decryptionKeyCache;
    private final Cache<String, SecretKey> lastDecryptionKeyCache;

    // Map of key name and encrypted gcm key, metadata pair which is sent with encrypted message
    private final ConcurrentHashMap<String, EncryptionKeyInfo> encryptedDataKeyMap;

    private static final SecureRandom secureRandom;
    static {
        SecureRandom rand;
        try {
            rand = SecureRandom.getInstance("NativePRNGNonBlocking");
        } catch (NoSuchAlgorithmException nsa) {
            rand = new SecureRandom();
        }
        secureRandom = rand;

        // Initial seed
        secureRandom.nextBytes(new byte[IV_LEN]);

        // Prefer SunJCE provider for AES-GCM for performance reason.
        // For cases where SunJCE is not available (e.g. non-hotspot JVM), use BouncyCastle as fallback.
        String sunJceProviderName = "SunJCE";
        if (Security.getProvider(sunJceProviderName) != null) {
            AESGCM_PROVIDER_NAME = sunJceProviderName;
        } else {
            AESGCM_PROVIDER_NAME = BouncyCastleProvider.PROVIDER_NAME;
        }

        // Add provider only if it's not in the JVM
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    // Thread-local instances for non-thread-safe JCA classes
    private static final FastThreadLocal<Cipher> THREAD_LOCAL_CIPHER = new FastThreadLocal<Cipher>() {
        @Override
        protected Cipher initialValue() throws Exception {
            return Cipher.getInstance(AESGCM, AESGCM_PROVIDER_NAME);
        }
    };

    private static final FastThreadLocal<KeyGenerator> THREAD_LOCAL_KEY_GENERATOR =
            new FastThreadLocal<KeyGenerator>() {
        @Override
        protected KeyGenerator initialValue() throws Exception {
            KeyGenerator kg = KeyGenerator.getInstance("AES");
            int aesKeyLength = Cipher.getMaxAllowedKeyLength("AES");
            if (aesKeyLength <= 128) {
                log.warn().attr("aesKeyLength", aesKeyLength)
                        .log("AES Cryptographic strength is limited."
                                + " Consider installing JCE Unlimited Strength Jurisdiction Policy Files");
                kg.init(aesKeyLength, secureRandom);
            } else {
                kg.init(256, secureRandom);
            }
            return kg;
        }
    };

    private static Cipher getAesGcmCipher() throws CryptoException {
        try {
            return THREAD_LOCAL_CIPHER.get();
        } catch (Exception e) {
            log.error().exceptionMessage(e).log("Failed to get AES-GCM cipher instance");
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
    }

    private static KeyGenerator getKeyGenerator() throws CryptoException {
        try {
            return THREAD_LOCAL_KEY_GENERATOR.get();
        } catch (Exception e) {
            log.error().exceptionMessage(e).log("Failed to get AES key generator instance");
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
    }

    private static SecretKey generateEncryptionKey() throws CryptoException {
        return getKeyGenerator().generateKey();
    }

    public MessageCryptoBc(String logCtx, boolean keyGenNeeded) {
        this.logCtx = logCtx;
        encryptedDataKeyMap = new ConcurrentHashMap<String, EncryptionKeyInfo>();
        decryptionKeyCache = Caffeine.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .weigher((SecretKeyCacheKey key, SecretKeySpec value) -> key.encryptedKeyBytes.length
                        + value.getEncoded().length)
                .maximumWeight(10 * 1024 * 1024) // 10MB upperbound
                .build();
        lastDecryptionKeyCache = Caffeine.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .maximumWeight(10 * 1024 * 1024) // 10MB upperbound
                .weigher((String key, SecretKey value) -> key.length() + value.getEncoded().length)
                .build();
        if (keyGenNeeded) {
            // Generate data key to encrypt messages
            try {
                encryptionKey = generateEncryptionKey();
            } catch (CryptoException e) {
                // retain same contract as before
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else {
                    throw new RuntimeException(e.getCause());
                }
            }
        }
    }

    public static PublicKey loadPublicKey(byte[] keyBytes) throws Exception {
        Reader keyReader = new StringReader(new String(keyBytes));
        PublicKey publicKey;
        try (PEMParser pemReader = new PEMParser(keyReader)) {
            Object pemObj = pemReader.readObject();
            JcaPEMKeyConverter pemConverter = new JcaPEMKeyConverter();
            SubjectPublicKeyInfo keyInfo;
            X9ECParameters ecParam = null;

            if (pemObj instanceof ASN1ObjectIdentifier) {
                // make sure this is EC Parameter we're handling. In which case
                // we'll store it and read the next object which should be our
                // EC Public Key
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
                ECPublicKeySpec keySpec = new ECPublicKeySpec(((ECPublicKey) publicKey).getQ(), ecSpec);
                publicKey = keyFactory.generatePublic(keySpec);
            }
        } catch (IOException | NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException e) {
            throw new Exception(e);
        }
        return publicKey;
    }

    private static PrivateKey loadPrivateKey(byte[] keyBytes) throws Exception {
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
            } else if (pemObj instanceof PrivateKeyInfo) {
                JcaPEMKeyConverter pemConverter = new JcaPEMKeyConverter();
                privateKey = pemConverter.getPrivateKey((PrivateKeyInfo) pemObj);
            }

            // if our private key is EC type and we have parameters specified
            // then we need to set it accordingly
            if (ecParam != null && ECDSA.equals(privateKey.getAlgorithm())) {
                ECParameterSpec ecSpec = new ECParameterSpec(ecParam.getCurve(), ecParam.getG(), ecParam.getN(),
                        ecParam.getH(), ecParam.getSeed());
                KeyFactory keyFactory = KeyFactory.getInstance(ECDSA, BouncyCastleProvider.PROVIDER_NAME);
                ECPrivateKeySpec keySpec = new ECPrivateKeySpec(((ECPrivateKey) privateKey).getS(), ecSpec);
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
    public void addPublicKeyCipher(Set<String> keyNames, CryptoKeyReader keyReader) throws CryptoException {
        // Rotate the encryption key each time this method is called
        encryptionKey = generateEncryptionKey();

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

        Cipher dataKeyCipher;
        byte[] encryptedKey;
        try {
            AlgorithmParameterSpec params = null;
            // Encrypt data key using public key
            if (RSA.equals(pubKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, BouncyCastleProvider.PROVIDER_NAME);
            } else if (ECDSA.equals(pubKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(ECIES, BouncyCastleProvider.PROVIDER_NAME);
                params = createIESParameterSpec();
            } else {
                String msg = logCtx + "Unsupported key type " + pubKey.getAlgorithm() + " for key " + keyName;
                log.error(msg);
                throw new PulsarClientException.CryptoException(msg);
            }
            if (params != null) {
                dataKeyCipher.init(Cipher.ENCRYPT_MODE, pubKey, params);
            } else {
                dataKeyCipher.init(Cipher.ENCRYPT_MODE, pubKey);
            }
            encryptedKey = dataKeyCipher.doFinal(encryptionKey.getEncoded());
        } catch (IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | NoSuchProviderException
                 | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            log.error().attr("logCtx", logCtx).attr("keyName", keyName)
                    .exceptionMessage(e).log("Failed to encrypt data key");
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
        EncryptionKeyInfo eki = new EncryptionKeyInfo(encryptedKey, keyInfo.getMetadata());
        encryptedDataKeyMap.put(keyName, eki);
    }

    // required since Bouncycastle 1.72 when using ECIES, it is required to pass in an IESParameterSpec
    public static IESParameterSpec createIESParameterSpec() {
        // the IESParameterSpec to use was discovered by debugging BouncyCastle 1.69 and running the
        // test org.apache.pulsar.client.api.SimpleProducerConsumerTest#testCryptoWithChunking
        return new IESParameterSpec(null, null, 128);
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
    public void encrypt(Set<String> encKeys, CryptoKeyReader keyReader,
                        Supplier<MessageMetadata> messageMetadataBuilderSupplier,
                        ByteBuffer payload, ByteBuffer outBuffer) throws PulsarClientException {
        MessageMetadata msgMetadata = messageMetadataBuilderSupplier.get();

        if (encKeys.isEmpty()) {
            outBuffer.put(payload);
            outBuffer.flip();
            return;
        }

        msgMetadata.clearEncryptionKeys();
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
                log.error().attr("logCtx", logCtx).attr("keyName", keyName)
                        .log("Failed to find encrypted Data key for key");
            }
        }

        // Create gcm param
        // TODO: Replace random with counter and periodic refreshing based on timer/counter value
        byte[] iv = new byte[IV_LEN];
        secureRandom.nextBytes(iv);
        GCMParameterSpec gcmParam = new GCMParameterSpec(tagLen, iv);

        // Update message metadata with encryption param
        msgMetadata.setEncryptionParam(iv);

        try {
            // Encrypt the data
            Cipher cipher = getAesGcmCipher();
            cipher.init(Cipher.ENCRYPT_MODE, encryptionKey, gcmParam);

            int maxLength = cipher.getOutputSize(payload.remaining());
            if (outBuffer.remaining() < maxLength) {
                throw new IllegalArgumentException("Outbuffer has not enough space available");
            }

            int bytesStored = cipher.doFinal(payload, outBuffer);
            outBuffer.flip();
            outBuffer.limit(bytesStored);
        } catch (IllegalBlockSizeException | BadPaddingException | InvalidKeyException
                | InvalidAlgorithmParameterException | ShortBufferException e) {
            log.error().attr("logCtx", logCtx).exception(e).log("Failed to encrypt message");
            throw new PulsarClientException.CryptoException(e.getMessage());
        }
    }

    private SecretKeySpec tryDecryptDataKey(String keyName, byte[] encryptedDataKey, List<KeyValue> encKeyMeta,
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
                log.error().attr("logCtx", logCtx).attr("keyName", keyName)
                        .log("Failed to load private key");
                return null;
            }
        } catch (Exception e) {
            log.error().attr("logCtx", logCtx).attr("keyName", keyName)
                    .exceptionMessage(e).log("Failed to decrypt data key to decrypt messages");
            return null;
        }

        // Decrypt data key to decrypt messages
        try {
            AlgorithmParameterSpec params = null;
            Cipher dataKeyCipher;
            // Decrypt data key using private key
            if (RSA.equals(privateKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(RSA_TRANS, BouncyCastleProvider.PROVIDER_NAME);
            } else if (ECDSA.equals(privateKey.getAlgorithm())) {
                dataKeyCipher = Cipher.getInstance(ECIES, BouncyCastleProvider.PROVIDER_NAME);
                params = createIESParameterSpec();
            } else {
                log.error().attr("keyType", privateKey.getAlgorithm()).attr("keyName", keyName)
                        .log("Unsupported key type");
                return null;
            }
            if (params != null) {
                dataKeyCipher.init(Cipher.DECRYPT_MODE, privateKey, params);
            } else {
                dataKeyCipher.init(Cipher.DECRYPT_MODE, privateKey);
            }
            byte[] dataKeyValue = dataKeyCipher.doFinal(encryptedDataKey);
            return new SecretKeySpec(dataKeyValue, "AES");

        } catch (Exception e) {
            log.error().attr("logCtx", logCtx).attr("keyName", keyName)
                    .exceptionMessage(e).log("Failed to decrypt data key to decrypt messages");
            return null;
        }
    }

    private boolean decryptData(SecretKey dataKeySecret, MessageMetadata msgMetadata,
                                ByteBuffer payload, ByteBuffer targetBuffer) {
        // unpack iv and encrypted data
        byte[] iv = msgMetadata.getEncryptionParam();

        GCMParameterSpec gcmParams = new GCMParameterSpec(tagLen, iv);
        try {
            // mark the buffers to allow resetting them in case of decryption failure
            payload.mark();
            targetBuffer.mark();

            Cipher cipher = getAesGcmCipher();
            cipher.init(Cipher.DECRYPT_MODE, dataKeySecret, gcmParams);
            int maxLength = cipher.getOutputSize(payload.remaining());
            if (targetBuffer.remaining() < maxLength) {
                throw new IllegalArgumentException("Target buffer size is too small");
            }
            int decryptedSize = cipher.doFinal(payload, targetBuffer);
            targetBuffer.flip();
            targetBuffer.limit(decryptedSize);
            return true;
        } catch (Exception e) {
            // reset the buffers so that decryption can be retried with the same buffers
            payload.reset();
            targetBuffer.reset();

            log.error().attr("logCtx", logCtx).exceptionMessage(e)
                    .log("Failed to decrypt message");
            return false;
        }
    }

    @Override
    public int getMaxOutputSize(int inputLen) {
        return inputLen + Math.max(inputLen, 512);
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
        String producerName = msgMetadata.hasProducerName() ? msgMetadata.getProducerName() : "__not_set__";

        // Pass 1: Try last used decryption key for this producer
        SecretKey lastKey = lastDecryptionKeyCache.getIfPresent(producerName);
        if (lastKey != null) {
            if (decryptData(lastKey, msgMetadata, payload, outBuffer)) {
                return true;
            } else {
                lastDecryptionKeyCache.invalidate(producerName);
            }
        }

        List<EncryptionKeys> encKeys = msgMetadata.getEncryptionKeysList();
        // Pass 2: Try cached keys (fast path — no CryptoKeyReader calls)
        for (EncryptionKeys encKey : encKeys) {
            SecretKeyCacheKey cacheKey = new SecretKeyCacheKey(encKey.getValue());
            SecretKey cachedKey = decryptionKeyCache.getIfPresent(cacheKey);
            if (cachedKey != null) {
                if (decryptData(cachedKey, msgMetadata, payload, outBuffer)) {
                    lastDecryptionKeyCache.put(producerName, cachedKey);
                    return true;
                }
            }
        }

        // Pass 3: Decrypt data keys via CryptoKeyReader (slow path)
        for (EncryptionKeys encKey : encKeys) {
            SecretKeySpec decryptedKey = tryDecryptDataKey(
                    encKey.getKey(), encKey.getValue(), encKey.getMetadatasList(), keyReader);
            if (decryptedKey != null) {
                SecretKeyCacheKey cacheKey = new SecretKeyCacheKey(encKey.getValue());
                decryptionKeyCache.put(cacheKey, decryptedKey);
                if (decryptData(decryptedKey, msgMetadata, payload, outBuffer)) {
                    lastDecryptionKeyCache.put(producerName, decryptedKey);
                    return true;
                }
            }
        }

        return false;
    }

    // key to be used in the cache
    private static final class SecretKeyCacheKey {
        private final byte[] encryptedKeyBytes;
        private final int hashCode;

        SecretKeyCacheKey(byte[] encryptedKeyBytes) {
            this.encryptedKeyBytes = encryptedKeyBytes.clone();
            this.hashCode = Arrays.hashCode(this.encryptedKeyBytes);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SecretKeyCacheKey)) {
                return false;
            }
            return Arrays.equals(encryptedKeyBytes, ((SecretKeyCacheKey) o).encryptedKeyBytes);
        }
    }
}

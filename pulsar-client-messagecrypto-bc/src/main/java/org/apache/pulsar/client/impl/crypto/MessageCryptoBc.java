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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.SecurityUtility;

@Slf4j
public class MessageCryptoBc implements MessageCrypto<MessageMetadata, MessageMetadata> {

    private static final String AESGCM = "AES/GCM/NoPadding";

    private static final String DATAKEY_ALGORITHM = "AES";

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


    private static final SecureRandom secureRandom;

    private static final String providerName;
    private final BcVersionSpecificCryptoUtility bcVersionSpecificCryptoUtilityDelegate;

    static {

        providerName = SecurityUtility.getProvider().getName();

        switch (providerName) {
            case SecurityUtility.BC:
                SecureRandom rand = null;
                try {
                    rand = SecureRandom.getInstance("NativePRNGNonBlocking", providerName);
                } catch (NoSuchAlgorithmException | NoSuchProviderException nsa) {
                    rand = new SecureRandom();
                }
                secureRandom = rand;
                break;
            case SecurityUtility.BC_FIPS:
                try {
                    secureRandom = SecureRandom.getInstance("NONCEANDIV", providerName);
                } catch (NoSuchAlgorithmException | NoSuchProviderException nsa) {
                    throw new RuntimeException(
                            "In BC FIPS mode, we expect the specific Random generators to be available!");
                }
                break;
            default:
                throw new IllegalStateException("Provider not handled for encryption: " + providerName);
        }

        // Initial seed
        secureRandom.nextBytes(new byte[IV_LEN]);
    }

    public MessageCryptoBc(String logCtx, boolean keyGenNeeded) {
        this(logCtx, keyGenNeeded, BcVersionSpecificCryptoUtility.INSTANCE);
    }

    /**
     * Alternative constructor - in case you want to provide a specific crypto utility instance, e.g. using KMS
     * to sign data keys
     *
     * TODO further refinements:
     *   - cryptoreader should be passed directly to the crypto utility instance as in some cases it is not really
     *     needed the traditional way (e.g.you cannot read private key from KMS), then the crypto util can decide
     *     what to do with them
     *   - data key generation should happen inside the crypto util (so people can do it in KMS not locally)
     *   - configurable data key regeneration schedule
     *
     * @param logCtx
     * @param keyGenNeeded
     * @param bcVersionSpecificCryptoUtilityDelegate
     */
    public MessageCryptoBc(String logCtx,
                           boolean keyGenNeeded,
                           BcVersionSpecificCryptoUtility bcVersionSpecificCryptoUtilityDelegate) {
        this.logCtx = logCtx;
        this.bcVersionSpecificCryptoUtilityDelegate = bcVersionSpecificCryptoUtilityDelegate;
        encryptedDataKeyMap = new ConcurrentHashMap<String, EncryptionKeyInfo>();
        dataKeyCache = CacheBuilder.newBuilder().expireAfterAccess(4, TimeUnit.HOURS)
                .build(new CacheLoader<ByteBuffer, SecretKey>() {

                    @Override
                    public SecretKey load(ByteBuffer key) {
                        return null;
                    }

                });

        try {

            cipher = Cipher.getInstance(AESGCM, providerName);
            // If keygen is not needed(e.g: consumer), data key will be decrypted from the message
            if (!keyGenNeeded) {

                digest = MessageDigest.getInstance("MD5", providerName);

                dataKey = null;
                return;
            }
            keyGenerator = KeyGenerator.getInstance(DATAKEY_ALGORITHM, providerName);
            int aesKeyLength = Cipher.getMaxAllowedKeyLength(DATAKEY_ALGORITHM);
            SecureRandom secureRandomForKeygen = null;

            switch (providerName) {
                case SecurityUtility.BC: {
                    //TODO is this prediction resistant for generating keys?
                    secureRandomForKeygen = secureRandom;
                    break;
                }
                case SecurityUtility.BC_FIPS: {
                    try {
                        //prediction resistant
                        secureRandomForKeygen = SecureRandom.getInstance("DEFAULT", providerName);
                    } catch (NoSuchAlgorithmException | NoSuchProviderException nsa) {
                        throw new RuntimeException(
                                "In BC FIPS mode, we expect the specific Random generators to be available!");
                    }
                    break;
                }
                default:
                    throw new IllegalStateException("Provider not handled for encryption: " + providerName);
            }

            if (aesKeyLength <= 128) {
                log.warn("{} AES Cryptographic strength is limited to {} bits. "
                                + "Consider installing JCE Unlimited Strength Jurisdiction Policy Files.",
                        logCtx, aesKeyLength);
                keyGenerator.init(aesKeyLength, secureRandomForKeygen);
            } else {
                keyGenerator.init(256, secureRandomForKeygen);
            }

        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {

            cipher = null;
            log.error("{} MessageCrypto initialization Failed {}", logCtx, e.getMessage());

        }

        // Generate data key to encrypt messages
        dataKey = keyGenerator.generateKey();

        iv = new byte[IV_LEN];
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
            pubKey = bcVersionSpecificCryptoUtilityDelegate.loadPublicKey(keyInfo.getKey());
        } catch (Exception e) {
            String msg = logCtx + "Failed to load public key " + keyName + ". " + e.getMessage();
            log.error(msg);
            throw new PulsarClientException.CryptoException(msg);
        }

        // Encrypt data key using public key
        byte[] encryptedKey = bcVersionSpecificCryptoUtilityDelegate.encryptDataKey(logCtx, keyName, pubKey, dataKey);

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
            privateKey = bcVersionSpecificCryptoUtilityDelegate.loadPrivateKey(keyInfo.getKey());
            if (privateKey == null) {
                log.error("{} Failed to load private key {}.", logCtx, keyName);
                return false;
            }
        } catch (Exception e) {
            log.error("{} Failed to decrypt data key {} to decrypt messages {}", logCtx, keyName, e.getMessage());
            return false;
        }

        // Decrypt data key to decrypt messages using private key
        Optional<SecretKey> decryptedKeyOpt = bcVersionSpecificCryptoUtilityDelegate
                .deCryptDataKey(DATAKEY_ALGORITHM, logCtx, keyName, privateKey, encryptedDataKey);
        if (!decryptedKeyOpt.isPresent()) {
            return false;
        } else {
            dataKey = decryptedKeyOpt.get();
        }

        byte[] keyDigest = digest.digest(encryptedDataKey);
        dataKeyCache.put(ByteBuffer.wrap(keyDigest), dataKey);
        return true;
    }

    private boolean decryptData(SecretKey dataKeySecret, MessageMetadata msgMetadata,
                                ByteBuffer payload, ByteBuffer targetBuffer) {

        // unpack iv and encrypted data
        iv = msgMetadata.getEncryptionParam();

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

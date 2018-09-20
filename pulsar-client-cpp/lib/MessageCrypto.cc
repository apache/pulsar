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

#include "LogUtils.h"
#include "MessageCrypto.h"

namespace pulsar {

DECLARE_LOG_OBJECT()

MessageCrypto::MessageCrypto(std::string& logCtx, bool keyGenNeeded)
    : dataKeyLen_(32),
      dataKey_(new unsigned char[dataKeyLen_]),
      tagLen_(16),
      ivLen_(12),
      iv_(new unsigned char[ivLen_]),
      logCtx_(logCtx) {
    SSL_library_init();
    SSL_load_error_strings();

    if (!keyGenNeeded) {
        mdCtx_ = EVP_MD_CTX_create();
        EVP_MD_CTX_init(mdCtx_);
        return;
    }

    RAND_bytes(dataKey_.get(), dataKeyLen_);
    RAND_bytes(iv_.get(), ivLen_);
}

MessageCrypto::~MessageCrypto() {}

RSA* MessageCrypto::loadPublicKey(std::string& pubKeyStr) {
    BIO* pubBio = NULL;
    RSA* rsaPub = NULL;

    pubBio = BIO_new_mem_buf((char*)pubKeyStr.c_str(), -1);
    if (pubBio == NULL) {
        LOG_ERROR(logCtx_ << " Failed to get memory for public key");
        return rsaPub;
    }

    rsaPub = PEM_read_bio_RSA_PUBKEY(pubBio, NULL, NULL, NULL);
    if (rsaPub == NULL) {
        LOG_ERROR(logCtx_ << " Failed to load public key");
    }

    BIO_free(pubBio);
    return rsaPub;
}

RSA* MessageCrypto::loadPrivateKey(std::string& privateKeyStr) {
    BIO* privBio = NULL;
    RSA* rsaPriv = NULL;

    privBio = BIO_new_mem_buf((char*)privateKeyStr.c_str(), -1);
    if (privBio == NULL) {
        LOG_ERROR(logCtx_ << " Failed to get memory for private key");
        return rsaPriv;
    }

    rsaPriv = PEM_read_bio_RSAPrivateKey(privBio, NULL, NULL, NULL);
    if (rsaPriv == NULL) {
        LOG_ERROR(logCtx_ << " Failed to load private key");
    }

    BIO_free(privBio);
    return rsaPriv;
}

bool MessageCrypto::getDigest(const std::string& keyName, const void* input, unsigned int inputLen,
                              unsigned char keyDigest[], unsigned int& digestLen) {
    if (EVP_DigestInit_ex(mdCtx_, EVP_md5(), NULL) != 1) {
        LOG_ERROR(logCtx_ << "Failed to initialize md5 digest for key " << keyName);
        return false;
    }

    digestLen = 0;
    if (EVP_DigestUpdate(mdCtx_, input, inputLen) != 1) {
        LOG_ERROR(logCtx_ << "Failed to get md5 hash for data key " << keyName);
        return false;
    }

    if (EVP_DigestFinal_ex(mdCtx_, keyDigest, &digestLen) != 1) {
        LOG_ERROR(logCtx_ << "Failed to finalize md hash for data key " << keyName);
        return false;
    }

    return true;
}

void MessageCrypto::removeExpiredDataKey() {
    boost::posix_time::ptime now = boost::posix_time::second_clock::universal_time();
    boost::posix_time::time_duration expireTime = boost::posix_time::hours(4);

    auto dataKeyCacheIter = dataKeyCache_.begin();
    while (dataKeyCacheIter != dataKeyCache_.end()) {
        auto dataKeyEntry = dataKeyCacheIter->second;
        boost::posix_time::time_duration td = now - dataKeyEntry.second;

        if ((now - dataKeyEntry.second) > expireTime) {
            dataKeyCache_.erase(dataKeyCacheIter++);
        } else {
            dataKeyCacheIter++;
        }
    }
}

std::string MessageCrypto::stringToHex(const char* inputStr, size_t len) {
    static const char* hexVals = "0123456789ABCDEF";

    std::string outHex;
    outHex.reserve(2 * len + 2);
    outHex.push_back('0');
    outHex.push_back('x');
    for (size_t i = 0; i < len; ++i) {
        const unsigned char c = *(inputStr + i);
        outHex.push_back(hexVals[c >> 4]);
        outHex.push_back(hexVals[c & 15]);
    }
    return outHex;
}

std::string MessageCrypto::stringToHex(const std::string& inputStr, size_t len) {
    return stringToHex(inputStr.c_str(), len);
}

Result MessageCrypto::addPublicKeyCipher(std::set<std::string>& keyNames,
                                         const CryptoKeyReaderPtr keyReader) {
    Lock lock(mutex_);

    // Generate data key
    RAND_bytes(dataKey_.get(), dataKeyLen_);
    if (PULSAR_UNLIKELY(logger()->isEnabled(Logger::DEBUG))) {
        std::string dataKeyStr(reinterpret_cast<char*>(dataKey_.get()), dataKeyLen_);
        std::string strHex = stringToHex(dataKeyStr, dataKeyStr.size());
        LOG_DEBUG(logCtx_ << "Generated Data key " << strHex);
    }

    Result result = ResultOk;
    for (auto it = keyNames.begin(); it != keyNames.end(); it++) {
        result = addPublicKeyCipher(*it, keyReader);
        if (result != ResultOk) {
            return result;
        }
    }
    return result;
}

Result MessageCrypto::addPublicKeyCipher(const std::string& keyName, const CryptoKeyReaderPtr keyReader) {
    if (keyName.empty()) {
        LOG_ERROR(logCtx_ << "Keyname is empty ");
        return ResultCryptoError;
    }

    // Read the public key and its info using callback
    StringMap keyMeta;
    EncryptionKeyInfo keyInfo;
    Result result = keyReader->getPublicKey(keyName, keyMeta, keyInfo);
    if (result != ResultOk) {
        LOG_ERROR(logCtx_ << "Failed to get public key from KeyReader for key " << keyName);
        return result;
    }

    RSA* pubKey = loadPublicKey(keyInfo.getKey());
    if (pubKey == NULL) {
        LOG_ERROR(logCtx_ << "Failed to load public key " << keyName);
        return ResultCryptoError;
    }
    LOG_DEBUG(logCtx_ << " Public key " << keyName << " loaded successfully.");

    int inSize = RSA_size(pubKey);
    boost::scoped_array<unsigned char> encryptedKey(new unsigned char[inSize]);

    int outSize =
        RSA_public_encrypt(dataKeyLen_, dataKey_.get(), encryptedKey.get(), pubKey, RSA_PKCS1_OAEP_PADDING);

    if (inSize != outSize) {
        LOG_ERROR(logCtx_ << "Ciphertext is length not matching input key length for key " << keyName);
        return ResultCryptoError;
    }
    std::string encryptedKeyStr(reinterpret_cast<char*>(encryptedKey.get()), inSize);
    std::shared_ptr<EncryptionKeyInfo> eki(new EncryptionKeyInfo());
    eki->setKey(encryptedKeyStr);
    eki->setMetadata(keyInfo.getMetadata());

    // Add a new entry or replace existing entry, if one is present.
    encryptedDataKeyMap_[keyName] = eki;

    if (PULSAR_UNLIKELY(logger()->isEnabled(Logger::DEBUG))) {
        std::string strHex = stringToHex(encryptedKeyStr, encryptedKeyStr.size());
        LOG_DEBUG(logCtx_ << " Data key encrypted for key " << keyName
                          << ". Encrypted key size = " << encryptedKeyStr.size() << ", value = " << strHex);
    }
    return ResultOk;
}

bool MessageCrypto::removeKeyCipher(const std::string& keyName) {
    if (!keyName.size()) {
        return false;
    }
    encryptedDataKeyMap_.erase(keyName);
    return true;
}

bool MessageCrypto::encrypt(std::set<std::string>& encKeys, const CryptoKeyReaderPtr keyReader,
                            proto::MessageMetadata& msgMetadata, SharedBuffer& payload,
                            SharedBuffer& encryptedPayload) {
    if (!encKeys.size()) {
        return false;
    }

    Lock lock(mutex_);

    // Update message metadata with encrypted data key
    for (auto it = encKeys.begin(); it != encKeys.end(); it++) {
        const std::string& keyName = *it;
        auto keyInfoIter = encryptedDataKeyMap_.find(keyName);

        if (keyInfoIter == encryptedDataKeyMap_.end()) {
            // Attempt to load the key. This will allow us to load keys as soon as
            // a new key is added to producer config
            Result result = addPublicKeyCipher(keyName, keyReader);
            if (result != ResultOk) {
                return false;
            }

            keyInfoIter = encryptedDataKeyMap_.find(keyName);

            if (keyInfoIter == encryptedDataKeyMap_.end()) {
                LOG_ERROR(logCtx_ << "Unable to find encrypted data key for " << keyName);
                return false;
            }
        }
        EncryptionKeyInfo* keyInfo = keyInfoIter->second.get();

        proto::EncryptionKeys* encKeys = proto::EncryptionKeys().New();
        encKeys->set_key(keyName);
        encKeys->set_value(keyInfo->getKey());
        if (PULSAR_UNLIKELY(logger()->isEnabled(Logger::DEBUG))) {
            std::string strHex = stringToHex(keyInfo->getKey(), keyInfo->getKey().size());
            LOG_DEBUG(logCtx_ << " Encrypted data key added for key " << keyName << ". Encrypted key size = "
                              << keyInfo->getKey().size() << ", value = " << strHex);
        }

        if (keyInfo->getMetadata().size()) {
            for (auto metaIter = keyInfo->getMetadata().begin(); metaIter != keyInfo->getMetadata().end();
                 metaIter++) {
                proto::KeyValue* keyValue = proto::KeyValue().New();
                keyValue->set_key(metaIter->first);
                keyValue->set_value(metaIter->second);
                encKeys->mutable_metadata()->AddAllocated(keyValue);
                LOG_DEBUG(logCtx_ << " Adding metadata for key " << keyName << ". Metadata key = "
                                  << metaIter->first << ", value =" << metaIter->second);
            }
        }

        msgMetadata.mutable_encryption_keys()->AddAllocated(encKeys);
    }

    // TODO: Replace random with counter and periodic refreshing based on timer/counter value
    RAND_bytes(iv_.get(), ivLen_);
    msgMetadata.set_encryption_param(reinterpret_cast<char*>(iv_.get()), ivLen_);

    EVP_CIPHER_CTX* cipherCtx = NULL;
    encryptedPayload = SharedBuffer::allocate(payload.readableBytes() + EVP_MAX_BLOCK_LENGTH + tagLen_);
    int encLen = 0;

    if (!(cipherCtx = EVP_CIPHER_CTX_new())) {
        LOG_ERROR(logCtx_ << " Failed to cipher ctx.");
        return false;
    }

    if (EVP_EncryptInit_ex(cipherCtx, EVP_aes_256_gcm(), NULL, dataKey_.get(), iv_.get()) != 1) {
        LOG_ERROR(logCtx_ << " Failed to init cipher ctx.");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }

    if (EVP_CIPHER_CTX_set_padding(cipherCtx, EVP_CIPH_NO_PADDING) != 1) {
        LOG_ERROR(logCtx_ << " Failed to set cipher padding.");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }

    if (EVP_EncryptUpdate(cipherCtx, reinterpret_cast<unsigned char*>(encryptedPayload.mutableData()),
                          &encLen, reinterpret_cast<unsigned const char*>(payload.data()),
                          payload.readableBytes()) != 1) {
        LOG_ERROR(logCtx_ << " Failed to encrypt payload.");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }
    encryptedPayload.bytesWritten(encLen);
    encLen = 0;

    if (EVP_EncryptFinal_ex(cipherCtx, reinterpret_cast<unsigned char*>(encryptedPayload.mutableData()),
                            &encLen) != 1) {
        LOG_ERROR(logCtx_ << " Failed to finalize encryption.");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }
    encryptedPayload.bytesWritten(encLen);

    if (EVP_CIPHER_CTX_ctrl(cipherCtx, EVP_CTRL_GCM_GET_TAG, tagLen_, encryptedPayload.mutableData()) != 1) {
        LOG_ERROR(logCtx_ << " Failed to get cipher tag info.");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }
    encryptedPayload.bytesWritten(tagLen_);
    if (PULSAR_UNLIKELY(logger()->isEnabled(Logger::DEBUG))) {
        std::string strPayloadHex = stringToHex(payload.data(), payload.readableBytes());
        std::string strHex = stringToHex(encryptedPayload.data(), encryptedPayload.readableBytes());
        LOG_DEBUG(logCtx_ << " Original size = " << payload.readableBytes() << ", value = " << strPayloadHex
                          << ". Encrypted size " << encryptedPayload.readableBytes()
                          << ", value =" << strHex);
    }

    EVP_CIPHER_CTX_free(cipherCtx);

    return true;
}

bool MessageCrypto::decryptDataKey(const std::string& keyName, const std::string& encryptedDataKey,
                                   const google::protobuf::RepeatedPtrField<proto::KeyValue>& encKeyMeta,
                                   const CryptoKeyReaderPtr keyReader) {
    StringMap keyMeta;
    for (auto iter = encKeyMeta.begin(); iter != encKeyMeta.end(); iter++) {
        keyMeta[iter->key()] = iter->value();
    }

    // Read the private key info using callback
    EncryptionKeyInfo keyInfo;
    keyReader->getPrivateKey(keyName, keyMeta, keyInfo);

    // Convert key from string to RSA key
    RSA* privKey = loadPrivateKey(keyInfo.getKey());
    if (privKey == NULL) {
        LOG_ERROR(logCtx_ << " Failed to load private key " << keyName);
        return false;
    }
    LOG_DEBUG(logCtx_ << " Private key " << keyName << " loaded successfully.");

    // Decrypt data key
    int outSize = RSA_private_decrypt(encryptedDataKey.size(),
                                      reinterpret_cast<unsigned const char*>(encryptedDataKey.c_str()),
                                      dataKey_.get(), privKey, RSA_PKCS1_OAEP_PADDING);

    if (outSize == -1) {
        LOG_ERROR(logCtx_ << "Failed to decrypt AES key for " << keyName);
        return false;
    }

    unsigned char keyDigest[EVP_MAX_MD_SIZE];
    unsigned int digestLen = 0;
    if (!getDigest(keyName, encryptedDataKey.c_str(), encryptedDataKey.size(), keyDigest, digestLen)) {
        LOG_ERROR(logCtx_ << "Failed to get digest for data key " << keyName);
        return false;
    }

    std::string keyDigestStr(reinterpret_cast<char*>(keyDigest), digestLen);
    std::string dataKeyStr(reinterpret_cast<char*>(dataKey_.get()), dataKeyLen_);
    dataKeyCache_[keyDigestStr] = make_pair(dataKeyStr, boost::posix_time::second_clock::universal_time());
    if (PULSAR_UNLIKELY(logger()->isEnabled(Logger::DEBUG))) {
        std::string strHex = stringToHex(dataKeyStr, dataKeyStr.size());
        LOG_DEBUG(logCtx_ << "Data key for key " << keyName << " decrypted. Decrypted data key is "
                          << strHex);
    }

    // Remove expired entries from the cache
    removeExpiredDataKey();
    return true;
}

bool MessageCrypto::decryptData(const std::string& dataKeySecret, const proto::MessageMetadata& msgMetadata,
                                SharedBuffer& payload, SharedBuffer& decryptedPayload) {
    // unpack iv and encrypted data
    msgMetadata.encryption_param().copy(reinterpret_cast<char*>(iv_.get()),
                                        msgMetadata.encryption_param().size());

    EVP_CIPHER_CTX* cipherCtx = NULL;
    decryptedPayload = SharedBuffer::allocate(payload.readableBytes() + EVP_MAX_BLOCK_LENGTH + tagLen_);
    if (PULSAR_UNLIKELY(logger()->isEnabled(Logger::DEBUG))) {
        std::string strHex = stringToHex(payload.data(), payload.readableBytes());
        LOG_DEBUG(logCtx_ << "Attempting to decrypt data with encrypted size " << payload.readableBytes()
                          << ", data = " << strHex);
    }

    if (!(cipherCtx = EVP_CIPHER_CTX_new())) {
        LOG_ERROR(logCtx_ << " Failed to get cipher ctx");
        return false;
    }

    if (!EVP_DecryptInit_ex(cipherCtx, EVP_aes_256_gcm(), NULL,
                            reinterpret_cast<unsigned const char*>(dataKeySecret.c_str()),
                            reinterpret_cast<unsigned const char*>(iv_.get()))) {
        LOG_ERROR(logCtx_ << " Failed to init decrypt cipher ctx");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }

    if (EVP_CIPHER_CTX_set_padding(cipherCtx, EVP_CIPH_NO_PADDING) != 1) {
        LOG_ERROR(logCtx_ << " Failed to set cipher padding");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }

    int cipherLen = payload.readableBytes() - tagLen_;
    int decLen = 0;
    if (!EVP_DecryptUpdate(cipherCtx, reinterpret_cast<unsigned char*>(decryptedPayload.mutableData()),
                           &decLen, reinterpret_cast<unsigned const char*>(payload.data()), cipherLen)) {
        LOG_ERROR(logCtx_ << " Failed to decrypt update");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    };
    decryptedPayload.bytesWritten(decLen);

    if (!EVP_CIPHER_CTX_ctrl(cipherCtx, EVP_CTRL_GCM_SET_TAG, tagLen_, (void*)(payload.data() + cipherLen))) {
        LOG_ERROR(logCtx_ << " Failed to set gcm tag");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }

    if (!EVP_DecryptFinal_ex(cipherCtx, reinterpret_cast<unsigned char*>(decryptedPayload.mutableData()),
                             &decLen)) {
        LOG_ERROR(logCtx_ << " Failed to finalize encrypted message");
        EVP_CIPHER_CTX_free(cipherCtx);
        return false;
    }
    decryptedPayload.bytesWritten(decLen);
    if (PULSAR_UNLIKELY(logger()->isEnabled(Logger::DEBUG))) {
        std::string strHex = stringToHex(decryptedPayload.data(), decryptedPayload.readableBytes());
        LOG_DEBUG(logCtx_ << "Data decrypted. Decrypted size = " << decryptedPayload.readableBytes()
                          << ", data = " << strHex);
    }

    EVP_CIPHER_CTX_free(cipherCtx);

    return true;
}

bool MessageCrypto::getKeyAndDecryptData(const proto::MessageMetadata& msgMetadata, SharedBuffer& payload,
                                         SharedBuffer& decryptedPayload) {
    SharedBuffer decryptedData;
    bool dataDecrypted = false;

    for (auto iter = msgMetadata.encryption_keys().begin(); iter != msgMetadata.encryption_keys().end();
         iter++) {
        const std::string& keyName = iter->key();
        const std::string& encDataKey = iter->value();
        unsigned char keyDigest[EVP_MAX_MD_SIZE];
        unsigned int digestLen = 0;
        getDigest(keyName, encDataKey.c_str(), encDataKey.size(), keyDigest, digestLen);

        std::string keyDigestStr(reinterpret_cast<char*>(keyDigest), digestLen);

        auto dataKeyCacheIter = dataKeyCache_.find(keyDigestStr);
        if (dataKeyCacheIter != dataKeyCache_.end()) {
            // Taking a small performance hit here if the hash collides. When it
            // retruns a different key, decryption fails. At this point, we would
            // call decryptDataKey to refresh the cache and come here again to decrypt.
            auto dataKeyEntry = dataKeyCacheIter->second;
            if (decryptData(dataKeyEntry.first, msgMetadata, payload, decryptedPayload)) {
                dataDecrypted = true;
                break;
            }
        } else {
            // First time, entry won't be present in cache
            LOG_DEBUG(logCtx_ << " Failed to decrypt data or data key is not in cache for "
                              << keyName + ". Will attempt to refresh.");
        }
    }
    return dataDecrypted;
}

bool MessageCrypto::decrypt(const proto::MessageMetadata& msgMetadata, SharedBuffer& payload,
                            const CryptoKeyReaderPtr keyReader, SharedBuffer& decryptedPayload) {
    // Attempt to decrypt using the existing key
    if (getKeyAndDecryptData(msgMetadata, payload, decryptedPayload)) {
        return true;
    }

    // Either first time, or decryption failed. Attempt to regenerate data key
    bool isDataKeyDecrypted = false;
    for (int index = 0; index < msgMetadata.encryption_keys_size(); index++) {
        const proto::EncryptionKeys& encKeys = msgMetadata.encryption_keys(index);

        const std::string& encDataKey = encKeys.value();
        const google::protobuf::RepeatedPtrField<proto::KeyValue>& encKeyMeta = encKeys.metadata();
        if (decryptDataKey(encKeys.key(), encDataKey, encKeyMeta, keyReader)) {
            isDataKeyDecrypted = true;
            break;
        }
    }

    if (!isDataKeyDecrypted) {
        // Unable to decrypt data key
        return false;
    }

    return getKeyAndDecryptData(msgMetadata, payload, decryptedPayload);
}

} /* namespace pulsar */

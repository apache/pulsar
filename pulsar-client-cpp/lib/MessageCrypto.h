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
#ifndef LIB_MESSAGECRYPTO_H_
#define LIB_MESSAGECRYPTO_H_

#include <iostream>
#include <map>
#include <set>
#include <mutex>
#include <boost/scoped_array.hpp>

#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/engine.h>

#include "SharedBuffer.h"
#include "ExecutorService.h"
#include "pulsar/CryptoKeyReader.h"
#include "PulsarApi.pb.h"

namespace pulsar {

class MessageCrypto {
   public:
    typedef std::map<std::string, std::string> StringMap;
    typedef std::map<std::string, std::pair<std::string, boost::posix_time::ptime>> DataKeyCacheMap;

    MessageCrypto(std::string& logCtx, bool keyGenNeeded);
    ~MessageCrypto();

    /*
     * Encrypt data key using the public key(s) in the argument. <p> If more than one key name is specified,
     * data key is encrypted using each of those keys. If the public key is expired or changed, application is
     * responsible to remove the old key and add the new key <p>
     *
     * @param keyNames List of public keys to encrypt data key
     * @param keyReader Implementation to read the key values
     * @return ResultOk if succeeded
     *
     */
    Result addPublicKeyCipher(const std::set<std::string>& keyNames, const CryptoKeyReaderPtr keyReader);

    /*
     * Remove a key <p> Remove the key identified by the keyName from the list of keys.<p>
     *
     * @param keyName Unique name to identify the key
     * @return true if succeeded, false otherwise
     */
    bool removeKeyCipher(const std::string& keyName);

    /*
     * Encrypt the payload using the data key and update message metadata with the keyname & encrypted data
     * key
     *
     * @param encKeys One or more public keys to encrypt data key
     * @param keyReader Implementation to read the key values
     * @param msgMetadata Message Metadata
     * @param payload Message which needs to be encrypted
     * @param encryptedPayload Contains encrypted payload if success
     *
     * @return true if success
     */
    bool encrypt(const std::set<std::string>& encKeys, const CryptoKeyReaderPtr keyReader,
                 proto::MessageMetadata& msgMetadata, SharedBuffer& payload, SharedBuffer& encryptedPayload);

    /*
     * Decrypt the payload using the data key. Keys used to encrypt data key can be retrieved from msgMetadata
     *
     * @param msgMetadata Message Metadata
     * @param payload Message which needs to be decrypted
     * @param keyReader KeyReader implementation to retrieve key value
     * @param decryptedPayload Contains decrypted payload if success
     *
     * @return true if success
     */
    bool decrypt(const proto::MessageMetadata& msgMetadata, SharedBuffer& payload,
                 const CryptoKeyReaderPtr keyReader, SharedBuffer& decryptedPayload);

   private:
    typedef std::unique_lock<std::mutex> Lock;
    std::mutex mutex_;

    int dataKeyLen_;
    boost::scoped_array<unsigned char> dataKey_;

    int tagLen_;
    int ivLen_;
    boost::scoped_array<unsigned char> iv_;

    std::string logCtx_;

    /* This cache uses the digest of encrypted data key as it's key. It's possible
     * for consumers to receive messages with data key encrypted using older or
     * newer version of public key. If we use the key name as the key for dataKeyCache,
     * we will end up decrypting data key way too often which is costly.
     */
    DataKeyCacheMap dataKeyCache_;

    // Map of key name and encrypted gcm key, metadata pair which is sent with encrypted message
    std::map<std::string, std::shared_ptr<EncryptionKeyInfo>> encryptedDataKeyMap_;

    EVP_MD_CTX* mdCtx_;

    RSA* loadPublicKey(std::string& pubKeyStr);
    RSA* loadPrivateKey(std::string& privateKeyStr);
    bool getDigest(const std::string& keyName, const void* input, unsigned int inputLen,
                   unsigned char keyDigest[], unsigned int& digestLen);
    void removeExpiredDataKey();

    Result addPublicKeyCipher(const std::string& keyName, const CryptoKeyReaderPtr keyReader);

    bool decryptDataKey(const std::string& keyName, const std::string& encryptedDataKey,
                        const google::protobuf::RepeatedPtrField<proto::KeyValue>& encKeyMeta,
                        const CryptoKeyReaderPtr keyReader);
    bool decryptData(const std::string& dataKeySecret, const proto::MessageMetadata& msgMetadata,
                     SharedBuffer& payload, SharedBuffer& decPayload);
    bool getKeyAndDecryptData(const proto::MessageMetadata& msgMetadata, SharedBuffer& payload,
                              SharedBuffer& decryptedPayload);
    std::string stringToHex(const std::string& inputStr, size_t len);
    std::string stringToHex(const char* inputStr, size_t len);
};

} /* namespace pulsar */

#endif /* LIB_MESSAGECRYPTO_H_ */

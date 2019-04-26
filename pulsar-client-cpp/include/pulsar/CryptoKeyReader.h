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
#ifndef CRYPTOKEYREADER_H_
#define CRYPTOKEYREADER_H_

#include <pulsar/defines.h>
#include <pulsar/Result.h>
#include <pulsar/EncryptionKeyInfo.h>

namespace pulsar {

class PULSAR_PUBLIC CryptoKeyReader {
   public:
    virtual ~CryptoKeyReader() {}

    /*
     * Return the encryption key corresponding to the key name in the argument
     * <p>
     * This method should be implemented to return the EncryptionKeyInfo. This method will be
     * called at the time of producer creation as well as consumer receiving messages.
     * Hence, application should not make any blocking calls within the implementation.
     * <p>
     *
     * @param keyName
     *            Unique name to identify the key
     * @param metadata
     *            Additional information needed to identify the key
     * @param encKeyInfo updated with details about the public key
     * @return Result ResultOk is returned for success
     *
     */
    virtual Result getPublicKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                                EncryptionKeyInfo& encKeyInfo) const = 0;

    /*
     * @param keyName
     *            Unique name to identify the key
     * @param metadata
     *            Additional information needed to identify the key
     * @param encKeyInfo updated with details about the private key
     * @return Result ResultOk is returned for success
     */
    virtual Result getPrivateKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                                 EncryptionKeyInfo& encKeyInfo) const = 0;

}; /* namespace pulsar */

typedef std::shared_ptr<CryptoKeyReader> CryptoKeyReaderPtr;
}  // namespace pulsar

#endif /* CRYPTOKEYREADER_H_ */

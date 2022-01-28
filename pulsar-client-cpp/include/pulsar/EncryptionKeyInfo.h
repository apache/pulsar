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
#ifndef ENCRYPTIONKEYINFO_H_
#define ENCRYPTIONKEYINFO_H_

#include <memory>
#include <iostream>
#include <map>
#include <pulsar/defines.h>

namespace pulsar {

class EncryptionKeyInfoImpl;
class PulsarWrapper;

typedef std::shared_ptr<EncryptionKeyInfoImpl> EncryptionKeyInfoImplPtr;

class PULSAR_PUBLIC EncryptionKeyInfo {
    /*
     * This object contains the encryption key and corresponding metadata which contains
     * additional information about the key such as version, timestammp
     */

   public:
    typedef std::map<std::string, std::string> StringMap;

    EncryptionKeyInfo();

    /**
     * EncryptionKeyInfo contains the encryption key and corresponding metadata which contains additional
     * information about the key, such as version and timestamp.
     */
    EncryptionKeyInfo(std::string key, StringMap& metadata);

    /**
     * @return the key of the message
     */
    std::string& getKey();

    /**
     * Set the key of the message for routing policy
     *
     * @param Key the key of the message for routing policy
     */
    void setKey(std::string key);

    /**
     * @return the metadata information
     */
    StringMap& getMetadata(void);

    /**
     * Set metadata information
     *
     * @param Metadata the information of metadata
     */
    void setMetadata(StringMap& metadata);

   private:
    explicit EncryptionKeyInfo(EncryptionKeyInfoImplPtr);

    EncryptionKeyInfoImplPtr impl_;

    friend class PulsarWrapper;
};

} /* namespace pulsar */

#endif /* ENCRYPTIONKEYINFO_H_ */

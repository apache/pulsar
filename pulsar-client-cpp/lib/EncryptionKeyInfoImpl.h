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
#ifndef LIB_ENCRYPTIONKEYINFOIMPL_H_
#define LIB_ENCRYPTIONKEYINFOIMPL_H_

#include <map>
#include <string>
#include <pulsar/defines.h>

namespace pulsar {

class PULSAR_PUBLIC EncryptionKeyInfoImpl {
   public:
    typedef std::map<std::string, std::string> StringMap;

    EncryptionKeyInfoImpl() = default;

    EncryptionKeyInfoImpl(std::string key, StringMap& metadata);

    std::string& getKey();

    void setKey(std::string key);

    StringMap& getMetadata(void);

    void setMetadata(StringMap& metadata);

   private:
    StringMap metadata_;
    std::string key_;
};

} /* namespace pulsar */

#endif /* LIB_ENCRYPTIONKEYINFOIMPL_H_ */

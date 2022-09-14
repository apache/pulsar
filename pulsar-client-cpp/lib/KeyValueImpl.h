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
#ifndef LIB_KEY_VALUEIMPL_H_
#define LIB_KEY_VALUEIMPL_H_

#include <pulsar/Message.h>
#include "SharedBuffer.h"
#include "Utils.h"

using namespace pulsar;

namespace pulsar {

class PULSAR_PUBLIC KeyValueImpl {
   public:
    KeyValueImpl();
    KeyValueImpl(const char* data, int length, KeyValueEncodingType keyValueEncodingType);
    KeyValueImpl(std::string&& key, std::string&& value);
    std::string getKey() const;
    const void* getValue() const;
    size_t getValueLength() const;
    std::string getValueAsString() const;
    SharedBuffer getContent(KeyValueEncodingType keyValueEncodingType);

   private:
    std::string key_;
    SharedBuffer valueBuffer_;
    Optional<SharedBuffer> contentBufferCache_;
};

} /* namespace pulsar */

#endif /* LIB_COMMANDS_H_ */

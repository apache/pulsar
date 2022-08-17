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
#ifndef KEY_VALUE_HPP_
#define KEY_VALUE_HPP_

#include <map>
#include <string>
#include <memory>
#include "defines.h"
#include "Schema.h"

namespace pulsar {

class KeyValueImpl;

class PULSAR_PUBLIC KeyValue {
   public:
    KeyValue();
    KeyValue(const std::string &data, const KeyValueEncodingType &keyValueEncodingType);
    KeyValue(const std::string &key, const std::string &value,
             const KeyValueEncodingType &keyValueEncodingType);
    std::string getContent() const;
    std::string getKeyData() const;
    KeyValueEncodingType getEncodingType() const;

   private:
    typedef std::shared_ptr<KeyValueImpl> KeyValueImplPtr;
    KeyValueImplPtr impl_;
};
}  // namespace pulsar

#endif /* KEY_VALUE_HPP_ */

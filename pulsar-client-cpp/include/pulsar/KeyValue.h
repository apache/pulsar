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

#include <string>
#include <memory>
#include "defines.h"
#include "Schema.h"

namespace pulsar {

class KeyValueImpl;

/**
 * Use to when the user uses key value schema.
 */
class PULSAR_PUBLIC KeyValue {
   public:
    /**
     * Constructor key value, according to keyValueEncodingType, whether key and value be encoded together.
     *
     * @param key  key data.
     * @param value value data.
     * @param keyValueEncodingType key value encoding type.
     */
    KeyValue(std::string &&key, std::string &&value);

    /**
     * Get the key of KeyValue.
     *
     * @return character stream for key
     */
    std::string getKey() const;

    /**
     * Get the value of the KeyValue.
     *
     *
     * @return the pointer to the KeyValue value
     */
    const void *getValue() const;

    /**
     * Get the value length of the keyValue.
     *
     * @return the length of the KeyValue value
     */
    size_t getValueLength() const;

    /**
     * Get string representation of the KeyValue value.
     *
     * @return the string representation of the KeyValue value
     */
    std::string getValueAsString() const;

   private:
    typedef std::shared_ptr<KeyValueImpl> KeyValueImplPtr;
    KeyValue(KeyValueImplPtr keyValueImplPtr);
    KeyValueImplPtr impl_;
    friend class Message;
    friend class MessageBuilder;
};
}  // namespace pulsar

#endif /* KEY_VALUE_HPP_ */

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
#include <pulsar/Schema.h>
#include "SharedBuffer.h"
#include "KeyValueImpl.h"

using namespace pulsar;

namespace pulsar {

KeyValueImpl::KeyValueImpl() {}

KeyValueImpl::KeyValueImpl(const char *data, int length, const KeyValueEncodingType &keyValueEncodingType) {
    keyValueEncodingType_ = keyValueEncodingType;
    if (keyValueEncodingType == KeyValueEncodingType::INLINE) {
        SharedBuffer buffer = SharedBuffer::wrap(const_cast<char *>(data), length);
        int keySize = buffer.readUnsignedInt();
        if (keySize != -1) {
            SharedBuffer keyContent = buffer.slice(0, keySize);
            keyContent_ = std::string(keyContent.data(), keySize);
            buffer.consume(keySize);
        }

        int valueSize = buffer.readUnsignedInt();
        if (valueSize != -1) {
            SharedBuffer valueContent = buffer.slice(0, valueSize);
            valueContent_ = std::string(valueContent.data(), valueSize);
        }
    } else {
        valueContent_ = std::string(data, length);
    }
}

KeyValueImpl::KeyValueImpl(const std::string &key, const std::string &value,
                           const KeyValueEncodingType &keyValueEncodingType)
    : keyContent_(key), valueContent_(value), keyValueEncodingType_(keyValueEncodingType) {}

std::string KeyValueImpl::getContent() const {
    if (keyValueEncodingType_ == KeyValueEncodingType::INLINE) {
        int keySize = keyContent_.length();
        int valueSize = valueContent_.length();

        int buffSize = sizeof(keySize) + keySize + sizeof(valueSize) + valueSize;
        SharedBuffer buffer = SharedBuffer::allocate(buffSize);
        buffer.writeUnsignedInt(keySize == 0 ? -1 : keySize);
        buffer.write(keyContent_.c_str(), keySize);
        buffer.writeUnsignedInt(valueSize == 0 ? -1 : valueSize);
        buffer.write(valueContent_.c_str(), valueSize);

        return std::string(buffer.data(), buffSize);
    } else {
        return valueContent_;
    }
}

std::string KeyValueImpl::getKey() const { return keyContent_; }

std::string KeyValueImpl::getValue() const { return valueContent_; }

KeyValueEncodingType KeyValueImpl::getEncodingType() const { return keyValueEncodingType_; }

}  // namespace pulsar

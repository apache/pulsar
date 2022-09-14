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

KeyValueImpl::KeyValueImpl(const char *data, int length, const KeyValueEncodingType &keyValueEncodingType) {
    keyValueEncodingType_ = keyValueEncodingType;
    if (keyValueEncodingType == KeyValueEncodingType::INLINE) {
        SharedBuffer buffer = SharedBuffer::wrap(const_cast<char *>(data), length);
        int keySize = buffer.readUnsignedInt();
        if (keySize != -1) {
            SharedBuffer keyContent = buffer.slice(0, keySize);
            key_ = std::string(keyContent.data(), keySize);
            buffer.consume(keySize);
        }

        int valueSize = buffer.readUnsignedInt();
        if (valueSize != -1) {
            valueBuffer_ = buffer.slice(0, valueSize);
        }
    } else {
        valueBuffer_ = SharedBuffer::wrap(const_cast<char *>(data), length);
    }
}

KeyValueImpl::KeyValueImpl(std::string &&key, std::string &&value,
                           const KeyValueEncodingType &keyValueEncodingType)
    : key_(key),
      valueBuffer_(SharedBuffer::take(std::move(value))),
      keyValueEncodingType_(keyValueEncodingType) {}

SharedBuffer KeyValueImpl::getContent() {
    if (contentBufferCache_.is_present()) {
        return contentBufferCache_.value();
    }
    if (keyValueEncodingType_ == KeyValueEncodingType::INLINE) {
        int keySize = key_.length();
        int valueSize = valueBuffer_.readableBytes();

        int buffSize = sizeof(keySize) + keySize + sizeof(valueSize) + valueSize;
        SharedBuffer buffer = SharedBuffer::allocate(buffSize);
        buffer.writeUnsignedInt(keySize == 0 ? -1 : keySize);
        buffer.write(key_.c_str(), keySize);

        buffer.writeUnsignedInt(valueSize == 0 ? -1 : valueSize);
        buffer.write(valueBuffer_.data(), valueSize);

        contentBufferCache_ = Optional<SharedBuffer>::of(buffer);
    } else {
        contentBufferCache_ =
            Optional<SharedBuffer>::of(SharedBuffer::copyFrom(valueBuffer_, valueBuffer_.readableBytes()));
    }
    return contentBufferCache_.value();
}

std::string KeyValueImpl::getKey() const { return key_; }

const void *KeyValueImpl::getValue() const { return valueBuffer_.data(); }

size_t KeyValueImpl::getValueLength() const { return valueBuffer_.readableBytes(); }

std::string KeyValueImpl::getValueAsString() const {
    return std::string(valueBuffer_.data(), valueBuffer_.readableBytes());
}

KeyValueEncodingType KeyValueImpl::getEncodingType() const { return keyValueEncodingType_; }

}  // namespace pulsar

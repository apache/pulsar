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
#include <pulsar/KeyValue.h>
#include <pulsar/Schema.h>
#include "SharedBuffer.h"

using namespace pulsar;

namespace pulsar {

class PULSAR_PUBLIC KeyValueImpl {
   public:
    std::string keyContent_;
    std::string valueContent_;
    KeyValueEncodingType keyValueEncodingType;

    KeyValueImpl(){};
    KeyValueImpl(std::string key, std::string value, KeyValueEncodingType keyValueEncodingType)
        : keyContent_(key), valueContent_(value), keyValueEncodingType(keyValueEncodingType){};
};

KeyValue::KeyValue() : impl_() {}

KeyValue::KeyValue(char *data, const int length, const KeyValueEncodingType &keyValueEncodingType) {
    impl_ = std::make_shared<KeyValueImpl>();
    impl_->keyValueEncodingType = keyValueEncodingType;
    if (impl_->keyValueEncodingType == KeyValueEncodingType::INLINE) {
        SharedBuffer buffer = SharedBuffer::wrap(data, length);
        int keySize = buffer.readUnsignedInt();
        if (keySize != -1) {
            SharedBuffer keyContent = buffer.slice(0, keySize);
            impl_->keyContent_ = std::string(keyContent.data(), keySize);
            buffer.consume(keySize);
        }

        int valueSize = buffer.readUnsignedInt();
        if (valueSize != -1) {
            SharedBuffer valueContent = buffer.slice(0, valueSize);
            impl_->valueContent_ = std::string(valueContent.data(), valueSize);
        }
    } else {
        impl_->valueContent_ = std::string(data, length);
    }
}

KeyValue::KeyValue(const std::string &key, const std::string &value,
                   const KeyValueEncodingType &keyValueEncodingType)
    : impl_(std::make_shared<KeyValueImpl>(key, value, keyValueEncodingType)) {}

std::string KeyValue::getContent() const {
    if (impl_->keyValueEncodingType == KeyValueEncodingType::INLINE) {
        std::string keyContent = impl_->keyContent_;
        std::string valueContent = impl_->valueContent_;
        int keySize = keyContent.length();
        int valueSize = valueContent.length();

        int buffSize = sizeof(keySize) + keySize + sizeof(valueSize) + valueSize;
        SharedBuffer buffer = SharedBuffer::allocate(buffSize);
        buffer.writeUnsignedInt(keySize == 0 ? -1 : keySize);
        buffer.write(keyContent.c_str(), keySize);
        buffer.writeUnsignedInt(valueSize == 0 ? -1 : valueSize);
        buffer.write(valueContent.c_str(), valueSize);

        return std::string(buffer.data(), buffSize);
    } else {
        return impl_->valueContent_;
    }
}

std::string KeyValue::getKeyData() const { return impl_->keyContent_; }
KeyValueEncodingType KeyValue::getEncodingType() const { return impl_->keyValueEncodingType; }

}  // namespace pulsar

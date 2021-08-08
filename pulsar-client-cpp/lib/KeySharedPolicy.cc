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
#include <lib/KeySharedPolicyImpl.h>

#include <algorithm>
#include <stdexcept>

namespace pulsar {

static const int DefaultHashRangeSize = 2 << 15;

KeySharedPolicy::KeySharedPolicy() : impl_(std::make_shared<KeySharedPolicyImpl>()) {}

KeySharedPolicy::~KeySharedPolicy() {}

KeySharedPolicy::KeySharedPolicy(const KeySharedPolicy &x) : impl_(x.impl_) {}

KeySharedPolicy &KeySharedPolicy::operator=(const KeySharedPolicy &x) {
    impl_ = x.impl_;
    return *this;
}

KeySharedPolicy &KeySharedPolicy::setKeySharedMode(KeySharedMode keySharedMode) {
    impl_->keySharedMode = keySharedMode;
    return *this;
}

KeySharedMode KeySharedPolicy::getKeySharedMode() const { return impl_->keySharedMode; }

KeySharedPolicy &KeySharedPolicy::setAllowOutOfOrderDelivery(bool allowOutOfOrderDelivery) {
    impl_->allowOutOfOrderDelivery = allowOutOfOrderDelivery;
    return *this;
}

bool KeySharedPolicy::isAllowOutOfOrderDelivery() const { return impl_->allowOutOfOrderDelivery; }

KeySharedPolicy &KeySharedPolicy::setStickyRanges(std::initializer_list<StickyRange> ranges) {
    if (ranges.size() == 0) {
        throw std::invalid_argument("Ranges for KeyShared policy must not be empty.");
    }
    for (StickyRange range : ranges) {
        if (range.first < 0 || range.second >= DefaultHashRangeSize) {
            throw std::invalid_argument("KeySharedPolicy Exception: Ranges must be [0, 65535].");
        }
        for (StickyRange range2 : ranges) {
            int start = std::max(range.first, range2.first);
            int end = std::min(range.second, range2.second);
            if (range != range2 && end >= start) {
                throw std::invalid_argument("Ranges for KeyShared policy with overlap.");
            }
        }
        for (StickyRange range : ranges) {
            impl_->ranges.push_back(range);
        }
    }
    return *this;
}

StickyRanges KeySharedPolicy::getStickyRanges() const { return impl_->ranges; }

KeySharedPolicy KeySharedPolicy::clone() const {
    KeySharedPolicy newConf;
    newConf.impl_.reset(new KeySharedPolicyImpl(*this->impl_));
    return newConf;
}

}  // namespace pulsar

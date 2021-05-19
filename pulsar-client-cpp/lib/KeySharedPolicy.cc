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

namespace pulsar {

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

KeySharedPolicy KeySharedPolicy::clone() const {
    KeySharedPolicy newConf;
    newConf.impl_.reset(new KeySharedPolicyImpl(*this->impl_));
    return newConf;
}

}  // namespace pulsar

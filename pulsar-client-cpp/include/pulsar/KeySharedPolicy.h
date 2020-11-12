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
#pragma once

#include <pulsar/defines.h>

#include <memory>

namespace pulsar {

/**
 * KeyShared mode of KeyShared subscription.
 */
enum KeySharedMode
{

    /**
     * Auto split while new consumer connected.
     */
    AUTO_SPLIT = 0,

    /**
     * New consumer with fixed hash range to attach the topic, if new consumer use conflict hash range with
     * exits consumers, new consumer will be rejected.
     */
    STICKY = 1
};

struct KeySharedPolicyImpl;

class PULSAR_PUBLIC KeySharedPolicy {
   public:
    KeySharedPolicy();
    ~KeySharedPolicy();

    KeySharedPolicy(const KeySharedPolicy&);
    KeySharedPolicy& operator=(const KeySharedPolicy&);

    /**
     * Create a new instance of KeySharedPolicy with the same
     * initial settings as the current one.
     */
    KeySharedPolicy clone() const;

    KeySharedPolicy& setKeySharedMode(KeySharedMode keySharedMode);
    KeySharedMode getKeySharedMode() const;

    KeySharedPolicy& setAllowOutOfOrderDelivery(bool allowOutOfOrderDelivery);
    bool isAllowOutOfOrderDelivery() const;

   private:
    std::shared_ptr<KeySharedPolicyImpl> impl_;
};
}  // namespace pulsar

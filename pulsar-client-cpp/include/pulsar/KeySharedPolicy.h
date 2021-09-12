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

#include <utility>
#include <vector>

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

typedef std::pair<int, int> StickyRange;
typedef std::vector<StickyRange> StickyRanges;

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

    /**
     * Configure the KeyShared mode of KeyShared subscription
     *
     * @param KeyShared mode
     * @see {@link #KeySharedMode}
     */
    KeySharedPolicy& setKeySharedMode(KeySharedMode keySharedMode);

    /**
     * @return the KeySharedMode of KeyShared subscription
     */
    KeySharedMode getKeySharedMode() const;

    /**
     * If it is enabled, it relaxes the ordering requirement and allows the broker to send out-of-order
     * messages in case of failures. This makes it faster for new consumers to join without being stalled by
     * an existing slow consumer.
     *
     * In this case, a single consumer still receives all keys, but they may come in different orders.
     *
     * @param allowOutOfOrderDelivery
     *            whether to allow for out of order delivery
     */
    KeySharedPolicy& setAllowOutOfOrderDelivery(bool allowOutOfOrderDelivery);

    /**
     * @return true if out of order delivery is enabled
     */
    bool isAllowOutOfOrderDelivery() const;

    /**
     * @param ranges used with sticky mode
     */
    KeySharedPolicy& setStickyRanges(std::initializer_list<StickyRange> ranges);

    /**
     * @return ranges used with sticky mode
     */
    StickyRanges getStickyRanges() const;

   private:
    std::shared_ptr<KeySharedPolicyImpl> impl_;
};
}  // namespace pulsar

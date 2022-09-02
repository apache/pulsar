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

#include <pulsar/BatchReceivePolicy.h>
#include "BatchReceivePolicyImpl.h"
#include "LogUtils.h"

using namespace pulsar;

namespace pulsar {

DECLARE_LOG_OBJECT()

BatchReceivePolicy::BatchReceivePolicy() : BatchReceivePolicy(-1, 10 * 1024 * 1024, 100) {}

BatchReceivePolicy::BatchReceivePolicy(int maxNumMessage, long maxNumBytes, long timeoutMs)
    : impl_(std::make_shared<BatchReceivePolicyImpl>()) {
    if (maxNumMessage <= 0 && maxNumBytes <= 0 && timeoutMs <= 0) {
        throw std::invalid_argument(
            "At least one of maxNumMessages, maxNumBytes and timeoutMs must be specified.");
    }
    if (maxNumMessage <= 0 && maxNumBytes <= 0) {
        impl_->maxNumMessage = -1;
        impl_->maxNumBytes = 10 * 1024 * 1024;
        LOG_WARN(
            "BatchReceivePolicy maxNumMessages and maxNumBytes is less than 0. Reset to default: "
            "maxNumMessage(-1), maxNumBytes(10 * 1024 * 10)");
    } else {
        impl_->maxNumMessage = maxNumMessage;
        impl_->maxNumBytes = maxNumBytes;
    }
    impl_->timeoutMs = timeoutMs;
}

long BatchReceivePolicy::getTimeoutMs() const { return impl_->timeoutMs; }

int BatchReceivePolicy::getMaxNumMessages() const { return impl_->maxNumMessage; }

long BatchReceivePolicy::getMaxNumBytes() const { return impl_->maxNumBytes; }

}  // namespace pulsar

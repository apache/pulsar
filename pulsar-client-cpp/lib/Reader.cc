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

#include <pulsar/Reader.h>

#include "Future.h"
#include "Utils.h"
#include "ReaderImpl.h"

namespace pulsar {

static const std::string EMPTY_STRING;

Reader::Reader() : impl_() {}

Reader::Reader(ReaderImplPtr impl) : impl_(impl) {}

const std::string& Reader::getTopic() const { return impl_ != NULL ? impl_->getTopic() : EMPTY_STRING; }

Result Reader::readNext(Message& msg) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->readNext(msg);
}

Result Reader::readNext(Message& msg, int timeoutMs) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->readNext(msg, timeoutMs);
}

Result Reader::close() {
    Promise<bool, Result> promise;
    closeAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Reader::closeAsync(ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->closeAsync(callback);
}

void Reader::hasMessageAvailableAsync(HasMessageAvailableCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized, false);
        return;
    }

    impl_->hasMessageAvailableAsync(callback);
}

Result Reader::hasMessageAvailable(bool& hasMessageAvailable) {
    Promise<Result, bool> promise;

    hasMessageAvailableAsync(WaitForCallbackValue<bool>(promise));
    return promise.getFuture().get(hasMessageAvailable);
}

}  // namespace pulsar

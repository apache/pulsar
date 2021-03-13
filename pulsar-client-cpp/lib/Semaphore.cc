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

#include "Semaphore.h"

namespace pulsar {

Semaphore::Semaphore(uint32_t limit) : limit_(limit), currentUsage_(0), mutex_(), condition_() {}

bool Semaphore::tryAcquire(int n) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (currentUsage_ + n > limit_) {
        return false;
    } else {
        currentUsage_ += n;
        return true;
    }
}

void Semaphore::acquire(int n) {
    std::unique_lock<std::mutex> lock(mutex_);

    while (currentUsage_ + n > limit_) {
        condition_.wait(lock);
    }

    currentUsage_ += n;
}

void Semaphore::release(int n) {
    std::lock_guard<std::mutex> lock(mutex_);
    currentUsage_ -= n;
    if (n == 1) {
        condition_.notify_one();
    } else {
        condition_.notify_all();
    }
}

uint32_t Semaphore::currentUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return currentUsage_;
}

}  // namespace pulsar

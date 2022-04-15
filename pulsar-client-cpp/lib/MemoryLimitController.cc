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

#include "MemoryLimitController.h"

namespace pulsar {

MemoryLimitController::MemoryLimitController(uint64_t memoryLimit)
    : memoryLimit_(memoryLimit), currentUsage_(0), mutex_(), condition_() {}

bool MemoryLimitController::tryReserveMemory(uint64_t size) {
    // Avoid CAS operation when size is 0
    if (size == 0) {
        return true;
    }
    while (true) {
        uint64_t current = currentUsage_;
        uint64_t newUsage = current + size;

        // We allow one request to go over the limit, to make the notification
        // path simpler and more efficient
        if (current > memoryLimit_ && memoryLimit_ > 0) {
            return false;
        }

        if (currentUsage_.compare_exchange_strong(current, newUsage)) {
            return true;
        }
    }
}

void MemoryLimitController::reserveMemory(uint64_t size) {
    if (!tryReserveMemory(size)) {
        std::unique_lock<std::mutex> lock(mutex_);

        // Check again, while holding the lock, to ensure we reserve attempt and the waiting for the condition
        // are synchronized.
        while (!tryReserveMemory(size)) {
            condition_.wait(lock);
        }
    }
}

void MemoryLimitController::releaseMemory(uint64_t size) {
    uint64_t oldUsage = currentUsage_.fetch_sub(size);
    uint64_t newUsage = oldUsage - size;

    if (newUsage + size > memoryLimit_ && newUsage <= memoryLimit_) {
        // We just crossed the limit. Now we have more space
        std::lock_guard<std::mutex> lock(mutex_);
        condition_.notify_all();
    }
}

uint64_t MemoryLimitController::currentUsage() const { return currentUsage_; }

}  // namespace pulsar

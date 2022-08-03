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

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace pulsar {
typedef std::function<void(void)> Trigger;

const static double MEMORY_THRESHOLD_FOR_RECEIVER_QUEUE_SIZE_EXPANSION5 = 0.75;

class MemoryLimitController {
   public:
    explicit MemoryLimitController(uint64_t memoryLimit);
    MemoryLimitController(uint64_t memoryLimit, uint64_t triggerThreshold, Trigger trigger);
    void forceReserveMemory(uint64_t size);
    bool tryReserveMemory(uint64_t size);
    bool reserveMemory(uint64_t size);
    void releaseMemory(uint64_t size);
    uint64_t currentUsage() const;
    double currentUsagePercent() const;

    void close();

   private:
    const uint64_t memoryLimit_;
    std::atomic<uint64_t> currentUsage_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool isClosed_ = false;
    const uint64_t triggerThreshold_;
    const Trigger trigger_;
    std::atomic_bool triggerRunning;
    void checkTrigger(uint64_t preUsage, uint64_t newUsage);
};

}  // namespace pulsar
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
#ifndef LIB_LATCH_H_
#define LIB_LATCH_H_

#include <memory>
#include <mutex>
#include <condition_variable>
#include <pulsar/defines.h>

namespace pulsar {

class PULSAR_PUBLIC Latch {
   public:
    Latch(int count);

    void countdown();

    void wait();

    template <typename Duration>
    bool wait(const Duration& timeout) {
        Lock lock(state_->mutex);
        return state_->condition.wait_for(lock, timeout, CountIsZero(state_->count));
    }

    int getCount();

   private:
    struct InternalState {
        std::mutex mutex;
        std::condition_variable condition;
        int count;
    };

    struct CountIsZero {
        const int& count_;

        CountIsZero(const int& count) : count_(count) {}

        bool operator()() const { return count_ == 0; }
    };

    typedef std::unique_lock<std::mutex> Lock;
    std::shared_ptr<InternalState> state_;
};
typedef std::shared_ptr<Latch> LatchPtr;
} /* namespace pulsar */

#endif /* LIB_LATCH_H_ */

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
#ifndef PERF_RATELIMITER_H_
#define PERF_RATELIMITER_H_

#include <chrono>
#include <thread>
#include <mutex>

namespace pulsar {

class RateLimiter {
 public:
    RateLimiter(double rate);

    void acquire();

    void acquire(int permits);

 private:
    RateLimiter(const RateLimiter&);
    RateLimiter& operator=(const RateLimiter&);
    typedef std::chrono::high_resolution_clock Clock;
    Clock::duration interval_;

    long storedPermits_;
    double maxPermits_;
    Clock::time_point nextFree_;
    std::mutex mutex_;
    typedef std::unique_lock<std::mutex> Lock;
};

RateLimiter::RateLimiter(double rate)
        : interval_(std::chrono::microseconds((long)(1e6 / rate))),
          storedPermits_(0.0),
          maxPermits_(rate),
          nextFree_() {
    assert(rate < 1e6 && "Exceeded maximum rate");
}

void RateLimiter::acquire() {
    acquire(1);
}

void RateLimiter::acquire(int permits) {
    Clock::time_point now = Clock::now();

    Lock lock(mutex_);

    if (now > nextFree_) {
        storedPermits_ = std::min<long>(maxPermits_,
                                        storedPermits_ + (now - nextFree_) / interval_);
        nextFree_ = now;
    }

    Clock::duration wait = nextFree_ - now;

    // Determine how many stored and fresh permits to consume
    long stored = std::min<long>(permits, storedPermits_);
    long fresh = permits - stored;

    // In the general RateLimiter, stored permits have no wait time,
    // and thus we only have to wait for however many fresh permits we consume
    Clock::duration next = fresh * interval_;
    nextFree_ += next;
    storedPermits_ -= stored;

    lock.unlock();

    if (wait != Clock::duration::zero()) {
        std::this_thread::sleep_for(wait);
    }
}

}  // pulsar

#endif /* PERF_RATELIMITER_H_ */

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
#include "Backoff.h"

namespace pulsar {

Backoff::Backoff(const TimeDuration& initial, const TimeDuration& max, const TimeDuration& mandatoryStop)
    : initial_(initial),
      max_(max),
      next_(initial),
      mandatoryStopMade_(false),
      mandatoryStop_(mandatoryStop)
#ifndef _MSC_VER
      ,
      randomSeed_(time(NULL))
#endif
{
}

TimeDuration Backoff::next() {
    TimeDuration current = next_;
    next_ = std::min(next_ * 2, max_);

    // Check for mandatory stop
    if (!mandatoryStopMade_) {
        const boost::posix_time::ptime& now = boost::posix_time::microsec_clock::universal_time();
        TimeDuration timeElapsedSinceFirstBackoff = boost::posix_time::milliseconds(0);
        if (initial_ == current) {
            firstBackoffTime_ = now;
        } else {
            timeElapsedSinceFirstBackoff = now - firstBackoffTime_;
        }
        if (timeElapsedSinceFirstBackoff + current > mandatoryStop_) {
            current = std::max(initial_, mandatoryStop_ - timeElapsedSinceFirstBackoff);
            mandatoryStopMade_ = true;
        }
    }
    // Add Randomness
#ifdef _MSC_VER
    int randomNumber = rand();
#else
    int randomNumber = rand_r(&randomSeed_);
#endif

    current = current - (current * (randomNumber % 10) / 100);
    return std::max(initial_, current);
}

void Backoff::reset() {
    next_ = initial_;
    mandatoryStopMade_ = false;
}

}  // namespace pulsar

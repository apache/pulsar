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
#include <algorithm>

namespace pulsar {

Backoff::Backoff(const TimeDuration& initial, const TimeDuration& max, const TimeDuration& mandatoryStop)
        : initial_(initial),
          max_(max),
          next_(initial),
          mandatoryStopMade_(false),
          mandatoryStop_(mandatoryStop) {
}

TimeDuration Backoff::next() {
    TimeDuration current = next_;
    next_ = std::min(next_ * 2, max_);

    static TimeDuration timeElapsedSinceDisconnection_;
    if (initial_ == current) {
        timeElapsedSinceDisconnection_ = boost::posix_time::milliseconds(0);
    }
    if (!mandatoryStopMade_ && timeElapsedSinceDisconnection_ + current > mandatoryStop_) {
        current = std::max(initial_, mandatoryStop_ - timeElapsedSinceDisconnection_);
        mandatoryStopMade_ = true;
    }
    timeElapsedSinceDisconnection_ += current;

    return current;
}

void Backoff::reset() {
    next_ = initial_;
    mandatoryStopMade_ = false;
}

}  //pulsar - namespace end

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
#ifndef _PULSAR_BACKOFF_HEADER_
#define _PULSAR_BACKOFF_HEADER_
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <stdlib.h> /* srand, rand */
#include <algorithm>
#include <time.h> /* time */
#include <pulsar/defines.h>

namespace pulsar {

typedef boost::posix_time::time_duration TimeDuration;

class PULSAR_PUBLIC Backoff {
   public:
    Backoff(const TimeDuration&, const TimeDuration&, const TimeDuration&);
    TimeDuration next();
    void reset();

   private:
    const TimeDuration initial_;
    const TimeDuration max_;
    TimeDuration next_;
    TimeDuration mandatoryStop_;
    boost::posix_time::ptime firstBackoffTime_;
    boost::random::mt19937 rng_;
    bool mandatoryStopMade_;
    friend class PulsarFriend;
};
}  // namespace pulsar

#endif  //_PULSAR_BACKOFF_HEADER_

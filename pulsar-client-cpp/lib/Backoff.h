/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef  _PULSAR_BACKOFF_HEADER_
#define _PULSAR_BACKOFF_HEADER_
#include <boost/date_time/posix_time/posix_time.hpp>

#pragma GCC visibility push(default)

namespace pulsar {

typedef boost::posix_time::time_duration TimeDuration;

class Backoff {
 public:
    Backoff(const TimeDuration& intial, const TimeDuration& max);
    TimeDuration next();
    void reset();
 private:
    TimeDuration initial_;
    TimeDuration max_;
    TimeDuration next_;
};
}

#pragma GCC visibility pop

#endif  //_PULSAR_BACKOFF_HEADER_

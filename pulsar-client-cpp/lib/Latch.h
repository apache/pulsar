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

#ifndef LIB_LATCH_H_
#define LIB_LATCH_H_

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#pragma GCC visibility push(default)

namespace pulsar {

class Latch {
 public:
    Latch(int count);

    void countdown();

    void wait();

    bool wait(const boost::posix_time::time_duration& timeout);

    int getCount();

 private:
    struct InternalState {
        boost::mutex mutex;
        boost::condition_variable condition;
        int count;
    };

    typedef boost::unique_lock<boost::mutex> Lock;
    boost::shared_ptr<InternalState> state_;
};
typedef boost::shared_ptr<Latch> LatchPtr;
} /* namespace pulsar */

#pragma GCC visibility pop

#endif /* LIB_LATCH_H_ */

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

#ifndef PULSAR_PUBLISHER_STATS_BASE_HEADER
#define PULSAR_PUBLISHER_STATS_BASE_HEADER
#include <pulsar/Message.h>
#include <pulsar/Result.h>
#include <time.h>
namespace pulsar {
class PublisherStatsBase {
 public:
    virtual void messageSent(const Message& msg) = 0;
    virtual void messageReceived(Result& res, timespec& publishTime) = 0;
    virtual void printStats() = 0;
};

typedef boost::shared_ptr<PublisherStatsBase> PublisherStatsBasePtr;
}

#endif // PULSAR_PUBLISHER_STATS_BASE_HEADER

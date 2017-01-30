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

#ifndef PULSAR_RR_MESSAGE_ROUTER_HEADER_
#define PULSAR_RR_MESSAGE_ROUTER_HEADER_

#include <pulsar/MessageRoutingPolicy.h>
#include <boost/functional/hash.hpp>
#include <boost/thread/mutex.hpp>

namespace pulsar {
    class RoundRobinMessageRouter : public MessageRoutingPolicy {
    public:
        RoundRobinMessageRouter (unsigned int numPartitions);
        virtual ~RoundRobinMessageRouter();
        virtual int getPartition(const Message& msg);
    private:
        boost::mutex mutex_;
        unsigned int prevPartition_;
        unsigned int numPartitions_;
    };
    typedef boost::hash<std::string> StringHash;
    typedef boost::unique_lock<boost::mutex> Lock;
}
#endif // PULSAR_RR_MESSAGE_ROUTER_HEADER_

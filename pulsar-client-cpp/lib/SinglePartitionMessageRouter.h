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

#ifndef PULSAR_SINGLE_PARTITION_MESSAGE_ROUTER_HEADER_
#define PULSAR_SINGLE_PARTITION_MESSAGE_ROUTER_HEADER_

#include <pulsar/MessageRoutingPolicy.h>
#include <boost/functional/hash.hpp>

namespace pulsar {

    class SinglePartitionMessageRouter : public MessageRoutingPolicy {
  public:
	SinglePartitionMessageRouter(unsigned int numPartitions);
        typedef boost::hash<std::string> StringHash;
        virtual ~SinglePartitionMessageRouter();
        virtual int getPartition(const Message& msg);
    private:
	unsigned int numPartitions_;
	int selectedSinglePartition_;
    };

}
#endif // PULSAR_SINGLE_PARTITION_MESSAGE_ROUTER_HEADER_

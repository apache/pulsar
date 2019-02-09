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
#ifndef LIB_UNACKEDMESSAGETRACKERINTERFACE_H_
#define LIB_UNACKEDMESSAGETRACKERINTERFACE_H_
#include <string>
#include <memory>
#include <set>
#include <algorithm>
#include <utility>
#include "pulsar/MessageId.h"
#include "lib/ClientImpl.h"
#include "lib/ConsumerImplBase.h"
#include <boost/asio.hpp>
#include <lib/LogUtils.h>
#include "lib/PulsarApi.pb.h"
#include <boost/asio/error.hpp>
namespace pulsar {

class UnAckedMessageTrackerInterface {
   public:
    virtual ~UnAckedMessageTrackerInterface() {}
    UnAckedMessageTrackerInterface() {}
    virtual bool add(const MessageId& m) = 0;
    virtual bool remove(const MessageId& m) = 0;
    virtual void removeMessagesTill(const MessageId& msgId) = 0;
    virtual void clear() = 0;
    // this is only for MultiTopicsConsumerImpl, when un-subscribe a single topic, should remove all it's
    // message.
    virtual void removeTopicMessage(const std::string& topic) = 0;
};

typedef std::unique_ptr<UnAckedMessageTrackerInterface> UnAckedMessageTrackerScopedPtr;
}  // namespace pulsar
#endif /* LIB_UNACKEDMESSAGETRACKERINTERFACE_H_ */

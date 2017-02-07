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

#ifndef LIB_UNACKEDMESSAGETRACKERINTERFACE_H_
#define LIB_UNACKEDMESSAGETRACKERINTERFACE_H_
#include <string>
#include <boost/shared_ptr.hpp>
#include <set>
#include <algorithm>
#include <utility>
#include "pulsar/MessageId.h"
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include "lib/ClientImpl.h"
#include "lib/ConsumerImplBase.h"
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <lib/LogUtils.h>
#include "lib/PulsarApi.pb.h"
#include <boost/thread/recursive_mutex.hpp>
#include <boost/asio/error.hpp>
namespace pulsar {

class UnAckedMessageTrackerInterface {
 public:
    virtual ~UnAckedMessageTrackerInterface() {
    }
    UnAckedMessageTrackerInterface() {
    }
    virtual bool add(const MessageId& m) = 0;
    virtual bool remove(const MessageId& m) = 0;
    virtual void removeMessagesTill(const MessageId& msgId) = 0;
};

typedef boost::scoped_ptr<UnAckedMessageTrackerInterface> UnAckedMessageTrackerScopedPtr;
}
#endif /* LIB_UNACKEDMESSAGETRACKERINTERFACE_H_ */

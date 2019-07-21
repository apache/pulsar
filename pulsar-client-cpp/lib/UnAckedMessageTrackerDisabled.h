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
#ifndef LIB_UNACKEDMESSAGETRACKERDISABLED_H_
#define LIB_UNACKEDMESSAGETRACKERDISABLED_H_
#include "lib/UnAckedMessageTrackerInterface.h"
namespace pulsar {

class UnAckedMessageTrackerDisabled : public UnAckedMessageTrackerInterface {
   public:
    bool add(const MessageId& m) { return false; }
    bool remove(const MessageId& m) { return false; }
    void removeMessagesTill(const MessageId& msgId) {}
    void removeTopicMessage(const std::string& topic) {}

    void clear() {}
};
}  // namespace pulsar
#endif /* LIB_UNACKEDMESSAGETRACKERDISABLED_H_ */

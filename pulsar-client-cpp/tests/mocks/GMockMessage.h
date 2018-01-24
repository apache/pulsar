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

#ifndef MOCK_MESSAGE_HPP_
#define MOCK_MESSAGE_HPP_

#include <gmock/gmock.h>
#include <pulsar/Message.h>

namespace pulsar {
// TODO: For the mock tests, we need to make all methods and destructor virtual in Message class
class GMockMessage : public Message {
   public:
    MOCK_CONST_METHOD0(hasPartitionKey, bool());

    MOCK_CONST_METHOD0(getPartitionKey, const std::string&());
};
}  // namespace pulsar

#endif  // MOCK_MESSAGE_HPP_

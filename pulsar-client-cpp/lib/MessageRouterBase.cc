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
#include "MessageRouterBase.h"

#include "BoostHash.h"
#include "JavaStringHash.h"
#include "Murmur3_32Hash.h"

namespace pulsar {
MessageRouterBase::MessageRouterBase(ProducerConfiguration::HashingScheme hashingScheme) {
    switch (hashingScheme) {
        case ProducerConfiguration::BoostHash:
            hash = HashPtr(new BoostHash());
            break;
        case ProducerConfiguration::JavaStringHash:
            hash = HashPtr(new JavaStringHash());
            break;
        case ProducerConfiguration::Murmur3_32Hash:
        default:
            hash = HashPtr(new Murmur3_32Hash());
            break;
    }
}
}  // namespace pulsar
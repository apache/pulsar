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
#ifndef PULSAR_CPP_CONSUMERTYPE_H
#define PULSAR_CPP_CONSUMERTYPE_H

namespace pulsar {
enum ConsumerType
{
    /**
     * There can be only 1 consumer on the same topic with the same consumerName
     */
    ConsumerExclusive,

    /**
     * Multiple consumers will be able to use the same consumerName and the messages
     *  will be dispatched according to a round-robin rotation between the connected consumers
     */
    ConsumerShared,

    /** Only one consumer is active on the subscription; Subscription can have N consumers
     *  connected one of which will get promoted to master if the current master becomes inactive
     */
    ConsumerFailover,

    /**
     * Multiple consumer will be able to use the same subscription and all messages with the same key
     * will be dispatched to only one consumer
     */
    ConsumerKeyShared
};
}

#endif  // PULSAR_CPP_CONSUMERTYPE_H

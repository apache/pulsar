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
#ifndef PULSAR_CONSUMEREVENTLISTENER_H_
#define PULSAR_CONSUMEREVENTLISTENER_H_

#include <pulsar/defines.h>

namespace pulsar {

class Consumer;

class PULSAR_PUBLIC ConsumerEventListener {
   public:
    virtual ~ConsumerEventListener(){};
    /**
     * @brief Notified when the consumer group is changed, and the consumer becomes active.
     *
     * @param consumer the consumer that originated the event
     * @param partitionId the id of the partition that beconmes active.
     */
    virtual void becameActive(Consumer consumer, int partitionId) = 0;

    /**
     * @brief Notified when the consumer group is changed, and the consumer is still inactive or becomes
     * inactive.
     *
     * @param consumer the consumer that originated the event
     * @param partitionId the id of the partition that is still inactive or becomes inactive.
     */
    virtual void becameInactive(Consumer consumer, int partitionId) = 0;
};
}  // namespace pulsar
#endif /* PULSAR_CONSUMEREVENTLISTENER_H_ */

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
#ifndef LIB_ACKGROUPINGTRACKERDISABLED_H_
#define LIB_ACKGROUPINGTRACKERDISABLED_H_

#include <cstdint>

#include "HandlerBase.h"
#include <pulsar/MessageId.h>
#include "AckGroupingTracker.h"

namespace pulsar {

/**
 * @class AckGroupingTrackerDisabled
 * ACK grouping tracker that does not tracker or group ACK requests. The ACK requests are diretly
 * sent to broker.
 */
class AckGroupingTrackerDisabled : public AckGroupingTracker {
   public:
    virtual ~AckGroupingTrackerDisabled() = default;

    /**
     * Constructing ACK grouping tracker for peresistent topics that disabled ACK grouping.
     * @param[in] handler the connection handler.
     * @param[in] consumerId consumer ID that this tracker belongs to.
     */
    AckGroupingTrackerDisabled(HandlerBase& handler, uint64_t consumerId);

    void addAcknowledge(const MessageId& msgId) override;
    void addAcknowledgeCumulative(const MessageId& msgId) override;

   private:
    //! The connection handler.
    HandlerBase& handler_;

    //! ID of the consumer that this tracker belongs to.
    uint64_t consumerId_;
};  // class AckGroupingTrackerDisabled

}  // namespace pulsar
#endif /* LIB_ACKGROUPINGTRACKERDISABLED_H_ */

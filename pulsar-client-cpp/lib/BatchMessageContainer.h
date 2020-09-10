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
/*
 * \class BatchMessageContainer
 *
 * \brief This class is a container for holding individual messages being published until they are batched and
 * sent to broker.
 *
 * \note This class is not thread safe.
 */

#ifndef LIB_BATCHMESSAGECONTAINER_H_
#define LIB_BATCHMESSAGECONTAINER_H_

#include "BatchMessageContainerBase.h"
#include "MessageAndCallbackBatch.h"

namespace pulsar {

class BatchMessageContainer : public BatchMessageContainerBase {
   public:
    BatchMessageContainer(const ProducerImpl& producer);

    ~BatchMessageContainer();

    size_t getNumBatches() const override { return 1; }

    bool isFirstMessageToAdd(const Message& msg) const override { return batch_.empty(); }

    bool add(const Message& msg, const SendCallback& callback) override;

    void clear() override;

    Result createOpSendMsg(OpSendMsg& opSendMsg, const FlushCallback& flushCallback) const override;

    std::vector<Result> createOpSendMsgs(std::vector<OpSendMsg>& opSendMsgs,
                                         const FlushCallback& flushCallback) const override;

    void serialize(std::ostream& os) const override;

   private:
    MessageAndCallbackBatch batch_;
    size_t numberOfBatchesSent_ = 0;
    double averageBatchSize_ = 0;
};

}  // namespace pulsar
#endif /* LIB_BATCHMESSAGECONTAINER_H_ */

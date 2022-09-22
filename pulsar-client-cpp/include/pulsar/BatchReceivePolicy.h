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
#ifndef BATCH_RECEIVE_POLICY_HPP_
#define BATCH_RECEIVE_POLICY_HPP_

#include <pulsar/defines.h>
#include <memory>

namespace pulsar {

struct BatchReceivePolicyImpl;

/**
 * Configuration for message batch receive {@link Consumer#batchReceive()} {@link
 * Consumer#batchReceiveAsync()}.
 *
 * <p>Batch receive policy can limit the number and bytes of messages in a single batch, and can specify a
 * timeout for waiting for enough messages for this batch.
 *
 * <p>A batch receive action is completed as long as any one of the
 * conditions (the batch has enough number or size of messages, or the waiting timeout is passed) are met.
 *
 * <p>Examples:
 * 1.If set maxNumMessages = 10, maxSizeOfMessages = 1MB and without timeout, it
 * means {@link Consumer#batchReceive()} will always wait until there is enough messages.
 * 2.If set maxNumberOfMessages = 0, maxNumBytes = 0 and timeout = 100ms, it
 * means {@link Consumer#batchReceive()} will wait for 100ms no matter whether there are enough messages.
 *
 * <p>Note:
 * Must specify messages limitation(maxNumMessages, maxNumBytes) or wait timeout.
 * Otherwise, {@link Messages} ingest {@link Message} will never end.
 *
 * @since 2.4.1
 */
class PULSAR_PUBLIC BatchReceivePolicy {
   public:
    /**
     * Default value: {maxNumMessage: -1, maxNumBytes: 10 * 1024 * 1024, timeoutMs: 100}
     */
    BatchReceivePolicy();

    /**
     *
     * @param maxNumMessage  Max num message, if less than 0, it means no limit.
     * @param maxNumBytes Max num bytes, if less than 0, it means no limit.
     * @param timeoutMs If less than 0, it means no limit.
     */
    BatchReceivePolicy(int maxNumMessage, long maxNumBytes, long timeoutMs);

    /**
     * Get max time out ms.
     *
     * @return
     */
    long getTimeoutMs() const;

    /**
     * Get the maximum number of messages.
     * @return
     */
    int getMaxNumMessages() const;

    /**
     * Get max num bytes.
     * @return
     */
    long getMaxNumBytes() const;

   private:
    std::shared_ptr<BatchReceivePolicyImpl> impl_;
};
}  // namespace pulsar

#endif /* BATCH_RECEIVE_POLICY_HPP_ */

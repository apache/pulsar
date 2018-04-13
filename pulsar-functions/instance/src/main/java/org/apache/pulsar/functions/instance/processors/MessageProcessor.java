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
package org.apache.pulsar.functions.instance.processors;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.InputMessage;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.ProcessingGuarantees;

/**
 * A processor that processes messages, used by {@link org.apache.pulsar.functions.instance.JavaInstanceRunnable}.
 */
@Evolving
public interface MessageProcessor extends AutoCloseable {

    static MessageProcessor create(PulsarClient client,
                                   FunctionDetails functionDetails,
                                   LinkedBlockingDeque<InputMessage> processQeueue) {
        FunctionDetails.SubscriptionType fnSubType = functionDetails.getSubscriptionType();
        ProcessingGuarantees processingGuarantees = functionDetails.getProcessingGuarantees();
        SubscriptionType subType;
        if (FunctionDetails.SubscriptionType.SHARED == fnSubType) {
            subType = SubscriptionType.Shared;
        } else if (FunctionDetails.SubscriptionType.EXCLUSIVE == fnSubType) {
            subType = SubscriptionType.Exclusive;
        } else {
            subType = SubscriptionType.Failover;
        }

        if (processingGuarantees == ProcessingGuarantees.EFFECTIVELY_ONCE) {
            return new EffectivelyOnceProcessor(
                client,
                functionDetails,
                processQeueue);
        } else if (processingGuarantees == ProcessingGuarantees.ATMOST_ONCE) {
            return new AtMostOnceProcessor(
                client,
                functionDetails,
                subType,
                processQeueue);
        } else {
            return new AtLeastOnceProcessor(
                client,
                functionDetails,
                subType,
                processQeueue);
        }
    }

    /**
     * Setup the input with a provided <i>processQueue</i>. The implementation of this processor is responsible for
     * setting up the input and passing the received messages from input to the provided <i>processQueue</i>.
     *
     * @param inputSerDe SerDe to deserialize messages from input.
     */
    void setupInput(Map<String, SerDe> inputSerDe)
        throws Exception;

    /**
     * Return the map of input consumers.
     *
     * @return the map of input consumers.
     */
    Map<String, Consumer> getInputConsumers();

    /**
     * Setup the output with a provided <i>outputSerDe</i>. The implementation of this processor is responsible for
     * setting up the output
     *
     * @param outputSerDe output serde.
     * @throws Exception
     */
    void setupOutput(SerDe outputSerDe) throws Exception;


    //
    // Methods that called on processing messages
    //

    /**
     * Method that is called before taking a message from process queue to process.
     *
     * <p>The implementation can do actions like failure recovery before actually taking messages out of process queue to
     * process.
     */
    void prepareDequeueMessageFromProcessQueue();

    /**
     * Method that is called before processing the input message <i>msg</i>.
     *
     * <p>The implementation can do actions like ensuring producer is ready for producing results.
     *
     * @param msg input message.
     * @return true if the msg can be process, otherwise false. If a processor can't process this message at this moment,
     *          this message will be put back to the process queue and process later.
     * @throws InterruptedException when the processor is interrupted on preparing processing message.
     */
    boolean prepareProcessMessage(InputMessage msg) throws InterruptedException;

    /**
     * Send the output message to the output topic. The output message is computed from <i>inputMsg</i>.
     *
     * <p>If the <i>outputMsgBuilder</i> is null, the implementation doesn't have to send any messages to the output.
     * The implementation can decide to acknowledge the input message based on its process guarantees.
     *
     * @param inputMsg input message
     * @param outputMsgBuilder output message builder. it can be null.
     */
    void sendOutputMessage(InputMessage inputMsg,
                           MessageBuilder outputMsgBuilder);

    /**
     * Handle the process exception when processing input message <i>inputMsg</i>.
     *
     * @param inputMsg input message
     * @param exception exception thrown when processing input message.
     */
    void handleProcessException(InputMessage inputMsg, Exception exception);


    @Override
    void close();

}

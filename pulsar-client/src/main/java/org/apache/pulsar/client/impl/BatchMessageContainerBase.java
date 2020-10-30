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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.BatchMessageContainer;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;

import java.io.IOException;
import java.util.List;

public interface BatchMessageContainerBase extends BatchMessageContainer {

    /**
     * Add message to the batch message container.
     *
     * @param msg message will add to the batch message container
     * @param callback message send callback
     * @return true if the batch is full, otherwise false
     */
    boolean add(MessageImpl<?> msg, SendCallback callback);

    /**
     * Check the batch message container have enough space for the message want to add.
     *
     * @param msg the message want to add
     * @return return true if the container have enough space for the specific message,
     *         otherwise return false.
     */
    boolean haveEnoughSpace(MessageImpl<?> msg);

    /**
     * Check the batch message container has same schema with the message want to add.
     *
     * @param msg the message want to add
     * @return return true if the container has same schema with the specific message,
     *         otherwise return false.
     */
    boolean hasSameSchema(MessageImpl<?> msg);

    /**
     * Set producer of the message batch container.
     *
     * @param producer producer
     */
    void setProducer(ProducerImpl<?> producer);

    /**
     * Create list of OpSendMsg, producer use OpSendMsg to send to the broker.
     *
     * @return list of OpSendMsg
     * @throws IOException
     */
    List<OpSendMsg> createOpSendMsgs() throws IOException;

    /**
     * Create OpSendMsg, producer use OpSendMsg to send to the broker.
     *
     * @return OpSendMsg
     * @throws IOException
     */
    OpSendMsg createOpSendMsg() throws IOException;

    /**
     * Check whether the added message belong to the same txn with batch message container.
     *
     * @param msg added message
     * @return belong to the same txn or not
     */
    boolean hasSameTxn(MessageImpl<?> msg);
}

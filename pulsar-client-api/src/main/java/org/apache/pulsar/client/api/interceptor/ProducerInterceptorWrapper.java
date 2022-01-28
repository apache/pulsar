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
package org.apache.pulsar.client.api.interceptor;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A wrapper for old style producer interceptor.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ProducerInterceptorWrapper implements ProducerInterceptor {
    private final org.apache.pulsar.client.api.ProducerInterceptor<?> innerInterceptor;

    public ProducerInterceptorWrapper(
            org.apache.pulsar.client.api.ProducerInterceptor<?> innerInterceptor) {
        this.innerInterceptor = innerInterceptor;
    }

    @Override
    public void close() {
        innerInterceptor.close();
    }

    @Override
    public boolean eligible(Message message) {
        return true;
    }

    @Override
    public Message beforeSend(Producer producer, Message message) {
        return innerInterceptor.beforeSend(producer, message);
    }

    @Override
    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId,
                                      Throwable exception) {
        innerInterceptor.onSendAcknowledgement(producer, message, msgId, exception);
    }

    @Override
    public void onPartitionsChange(String topicName, int partitions) {
        innerInterceptor.onPartitionsChange(topicName, partitions);
    }
}

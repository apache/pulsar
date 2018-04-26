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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.connect.core.Record;
import org.apache.pulsar.connect.core.Source;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.PulsarConfig;
import org.apache.pulsar.functions.instance.PulsarSource;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Reflections;

/**
 * The base implementation of {@link MessageProcessor}.
 */
@Slf4j
abstract class MessageProcessorBase implements MessageProcessor {

    protected final PulsarClient client;
    protected final FunctionDetails functionDetails;
    protected final SubscriptionType subType;

    @Getter
    protected Source source;

    protected List<String> topics;

    protected MessageProcessorBase(PulsarClient client,
                                   FunctionDetails functionDetails,
                                   SubscriptionType subType) {
        this.client = client;
        this.functionDetails = functionDetails;
        this.subType = subType;
        this.topics = new LinkedList<>();
        this.topics.addAll(this.functionDetails.getInputsList());
        this.topics.addAll(this.functionDetails.getCustomSerdeInputsMap().keySet());
    }

    //
    // Input
    //

    @Override
    public void setupInput(Map<String, SerDe> inputSerDe) throws Exception {

        org.apache.pulsar.functions.proto.Function.ConnectorDetails connectorDetails = this.functionDetails.getSource();
        Object object;
        if (connectorDetails.getClassName().equals(PulsarSource.class.getName())) {
            PulsarConfig pulsarConfig = PulsarConfig.builder()
                    .topicToSerdeMap(inputSerDe)
                    .subscription(FunctionDetailsUtils.getFullyQualifiedName(this.functionDetails))
                    .processingGuarantees(this.functionDetails.getProcessingGuarantees())
                    .subscriptionType(this.subType)
                    .build();
            Object[] params = {this.client, pulsarConfig};
            Class[] paramTypes = {PulsarClient.class, PulsarConfig.class};
            object = Reflections.createInstance(
                    connectorDetails.getClassName(),
                    PulsarSource.class.getClassLoader(), params, paramTypes);

        } else {
            object = Reflections.createInstance(
                    connectorDetails.getClassName(),
                    Thread.currentThread().getContextClassLoader());
        }

        Class<?>[] typeArgs;
        if (object instanceof Source) {
            typeArgs = TypeResolver.resolveRawArguments(Source.class, object.getClass());
            assert typeArgs.length > 0;
        } else {
            throw new RuntimeException("Source does not implement correct interface");
        }
        this.source = (Source) object;

        try {
            this.source.open(connectorDetails.getConfigsMap());
        } catch (Exception e) {
            log.info("Error occurred executing open for source: {}",
                    this.functionDetails.getSource().getClassName(), e);
        }

    }

    protected SubscriptionType getSubscriptionType() {
        return subType;
    }

    public Record recieveMessage() throws Exception {
        return this.source.read();
    }

    /**
     * Method called when a message is received from input after being put into the process queue.
     *
     * <p>The processor implementation can make a decision to process the message based on its processing guarantees.
     * for example, an at-most-once processor can ack the message immediately.
     *
     * @param record input message.
     */
    @Override
    public void postReceiveMessage(Record record) {}

    //
    // Output
    //

    @Override
    public void setupOutput(SerDe outputSerDe) throws Exception {
        String outputTopic = functionDetails.getOutput();
        if (outputTopic != null
                && !functionDetails.getOutput().isEmpty()
                && outputSerDe != null) {
            log.info("Starting producer for output topic {}", outputTopic);
            initializeOutputProducer(outputTopic);
        }
    }

    protected abstract void initializeOutputProducer(String outputTopic) throws Exception;

    //
    // Process
    //

    @Override
    public void close() {

        try {
            this.source.close();
        } catch (Exception e) {
            log.warn("Failed to close source {}", this.source, e);
        }
    }
}

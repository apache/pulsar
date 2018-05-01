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

import com.google.gson.Gson;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.connect.core.Record;
import org.apache.pulsar.connect.core.Source;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.source.PulsarConfig;
import org.apache.pulsar.functions.source.PulsarSource;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Reflections;

/**
 * The base implementation of {@link MessageProcessor}.
 */
@Slf4j
abstract class MessageProcessorBase implements MessageProcessor {

    protected final PulsarClient client;
    protected final FunctionDetails functionDetails;

    @Getter
    protected Source source;


    protected MessageProcessorBase(PulsarClient client,
                                   FunctionDetails functionDetails) {
        this.client = client;
        this.functionDetails = functionDetails;
    }

    //
    // Input
    //

    @Override
    public void setupInput(Class<?> inputType) throws Exception {

        org.apache.pulsar.functions.proto.Function.SourceSpec sourceSpec = this.functionDetails.getSource();
        Object object;
        if (sourceSpec.getClassName().equals(PulsarSource.class.getName())) {

            PulsarConfig pulsarConfig = new PulsarConfig();
            pulsarConfig.setTopicSerdeClassNameMap(this.functionDetails.getSource().getTopicsToSerDeClassNameMap());
            pulsarConfig.setSubscriptionName(FunctionDetailsUtils.getFullyQualifiedName(this.functionDetails));
            pulsarConfig.setProcessingGuarantees(
                    FunctionConfig.ProcessingGuarantees.valueOf(this.functionDetails.getProcessingGuarantees().name()));
            pulsarConfig.setSubscriptionType(
                    FunctionConfig.SubscriptionType.valueOf(this.functionDetails.getSource().getSubscriptionType().name()));
            pulsarConfig.setTypeClassName(inputType.getName());

            Object[] params = {this.client, pulsarConfig};
            Class[] paramTypes = {PulsarClient.class, PulsarConfig.class};

            object = Reflections.createInstance(
                    sourceSpec.getClassName(),
                    PulsarSource.class.getClassLoader(), params, paramTypes);

        } else {
            object = Reflections.createInstance(
                    sourceSpec.getClassName(),
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
            this.source.open(new Gson().fromJson(sourceSpec.getConfigs(), Map.class));
        } catch (Exception e) {
            log.info("Error occurred executing open for source: {}",
                    this.functionDetails.getSource().getClassName(), e);
        }

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
        String outputTopic = functionDetails.getSink().getTopic();
        if (outputTopic != null
                && !outputTopic.isEmpty()
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

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
package org.apache.pulsar.functions.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.io.core.SourceContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class MultiConsumerPulsarSource<T> extends PushPulsarSource<T> implements MessageListener<T> {

    private final MultiConsumerPulsarSourceConfig pulsarSourceConfig;
    private final ClassLoader functionClassLoader;
    private final List<Consumer<T>> inputConsumers = new LinkedList<>();

    public MultiConsumerPulsarSource(PulsarClient pulsarClient,
                                     MultiConsumerPulsarSourceConfig pulsarSourceConfig,
                                     Map<String, String> properties,
                                     ClassLoader functionClassLoader) {
        super(pulsarClient, pulsarSourceConfig, properties, functionClassLoader);
        this.pulsarSourceConfig = pulsarSourceConfig;
        this.functionClassLoader = functionClassLoader;
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        log.info("Opening pulsar source with config: {}", pulsarSourceConfig);
        Map<String, PulsarSourceConsumerConfig<T>> configs = setupConsumerConfigs();

        for (Map.Entry<String, PulsarSourceConsumerConfig<T>> e : configs.entrySet()) {
            String topic = e.getKey();
            PulsarSourceConsumerConfig<T> conf = e.getValue();
            log.info("Creating consumers for topic : {}, schema : {}, schemaInfo: {}",
                    topic, conf.getSchema(), conf.getSchema().getSchemaInfo());

            ConsumerBuilder<T> cb = createConsumeBuilder(topic, conf);

            //messageListener is annotated with @JsonIgnore,so setting messageListener should be put behind loadConf
            cb.messageListener(this);

            Consumer<T> consumer = cb.subscribeAsync().join();
            inputConsumers.add(consumer);
        }
    }

    @Override
    public void received(Consumer<T> consumer, Message<T> message) {
        consume(buildRecord(consumer, message));
    }

    @Override
    public void close() throws Exception {
        if (inputConsumers != null ) {
            inputConsumers.forEach(consumer -> {
                try {
                    consumer.close();
                } catch (PulsarClientException e) {
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, PulsarSourceConsumerConfig<T>> setupConsumerConfigs() throws ClassNotFoundException {
        Map<String, PulsarSourceConsumerConfig<T>> configs = new TreeMap<>();

        Class<?> typeArg = Reflections.loadClass(this.pulsarSourceConfig.getTypeClassName(),
                this.functionClassLoader);

        checkArgument(!Void.class.equals(typeArg), "Input type of Pulsar Function cannot be Void");

        // Check new config with schema types or classnames
        pulsarSourceConfig.getTopicSchema().forEach((topic, conf) -> {
            configs.put(topic, buildPulsarSourceConsumerConfig(topic, conf, typeArg));
        });

        return configs;
    }

    @Override
    public List<Consumer<T>> getInputConsumers() {
        return inputConsumers;
    }

}

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
package org.apache.pulsar.broker.events;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.io.IOException;
import java.util.Map;

/**
 * A collection of broker event listener.
 */
@Slf4j
public class BrokerEventListeners implements BrokerEventListener {

    private final Map<String, SafeBrokerEventListenerWithClassLoader> listeners;

    private static final BrokerEventListeners DISABLED = new BrokerEventListenersDisabled();

    public BrokerEventListeners(Map<String, SafeBrokerEventListenerWithClassLoader> listeners) {
        this.listeners = listeners;
    }

    private static class BrokerEventListenersDisabled extends BrokerEventListeners {

        public BrokerEventListenersDisabled() {
            super(null);
        }

        @Override
        public void onNewProducer(PulsarApi.CommandProducer command) {
            //No-op
        }

        @Override
        public void onSubscribe(PulsarApi.CommandSubscribe command) {
            //No-op
        }

        @Override
        public void onUnsubscribe(PulsarApi.CommandUnsubscribe command) {
            //No-op
        }

        @Override
        public void onCloseProducer(PulsarApi.CommandCloseProducer command) {
            //No-op
        }

        @Override
        public void onCloseConsumer(PulsarApi.CommandCloseConsumer command) {
            //No-op
        }

        @Override
        public void close() {
            //No-op
        }
    }

    /**
     * Load the broker event listener for the given <tt>listener</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of broker event listener
     */
    public static BrokerEventListeners load(ServiceConfiguration conf) throws IOException {
        BrokerEventListenerDefinitions definitions =
                BrokerEventListenerUtils.searchForListeners(conf.getBrokerListenersDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, SafeBrokerEventListenerWithClassLoader> builder = ImmutableMap.builder();

        conf.getBrokerListeners().forEach(listenerName -> {

            BrokerEventListenerMetadata definition = definitions.listeners().get(listenerName);
            if (null == definition) {
                throw new RuntimeException("No broker listener is found for name `" + listenerName
                        + "`. Available broker listeners are : " + definitions.listeners());
            }

            SafeBrokerEventListenerWithClassLoader listener;
            try {
                listener = BrokerEventListenerUtils.load(definition, conf.getNarExtractionDirectory());
                if (listener != null) {
                    builder.put(listenerName, listener);
                }
                log.info("Successfully loaded broker listener for name `{}`", listenerName);
            } catch (IOException e) {
                log.error("Failed to load the broker listener for name `" + listenerName + "`", e);
                throw new RuntimeException("Failed to load the broker listener for name `" + listenerName + "`");
            }
        });

        Map<String, SafeBrokerEventListenerWithClassLoader> listeners = builder.build();
        if (listeners != null && !listeners.isEmpty()) {
            return new BrokerEventListeners(listeners);
        } else {
            return DISABLED;
        }
    }

    @Override
    public void onNewProducer(PulsarApi.CommandProducer command) {
        listeners.values().forEach(listener -> listener.onNewProducer(command));
    }

    @Override
    public void onSubscribe(PulsarApi.CommandSubscribe command) {
        listeners.values().forEach(listener -> listener.onSubscribe(command));
    }

    @Override
    public void onUnsubscribe(PulsarApi.CommandUnsubscribe command) {
        listeners.values().forEach(listener -> listener.onUnsubscribe(command));
    }

    @Override
    public void onCloseProducer(PulsarApi.CommandCloseProducer command) {
        listeners.values().forEach(listener -> listener.onCloseProducer(command));
    }

    @Override
    public void onCloseConsumer(PulsarApi.CommandCloseConsumer command) {
        listeners.values().forEach(listener -> listener.onCloseConsumer(command));
    }

    @Override
    public void close() {
        listeners.values().forEach(listener -> listener.close());
    }
}

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

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.nar.NarClassLoader;

import java.io.IOException;

/**
 * A broker listener with it's classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
public class SafeBrokerEventListenerWithClassLoader implements BrokerEventListener {

    private final BrokerEventListener interceptor;
    private final NarClassLoader classLoader;


    @Override
    public void onNewProducer(PulsarApi.CommandProducer command) {
        try {
            this.interceptor.onNewProducer(command);
        } catch (Throwable e) {
            log.error("Fail to execute on new producer on broker listener", e);
        }
    }

    @Override
    public void onSubscribe(PulsarApi.CommandSubscribe command) {
        try {
            this.interceptor.onSubscribe(command);
        } catch (Throwable e) {
            log.error("Fail to execute on subscribe on broker listener", e);
        }
    }

    @Override
    public void onUnsubscribe(PulsarApi.CommandUnsubscribe command) {
        try {
            this.interceptor.onUnsubscribe(command);
        } catch (Throwable e) {
            log.error("Fail to execute on unsubscribe on broker listener", e);
        }
    }

    @Override
    public void onCloseProducer(PulsarApi.CommandCloseProducer command) {
        try {
            this.interceptor.onCloseProducer(command);
        } catch (Throwable e) {
            log.error("Fail to execute on close producer on broker listener", e);
        }
    }

    @Override
    public void onCloseConsumer(PulsarApi.CommandCloseConsumer command) {
        try {
            this.interceptor.onCloseConsumer(command);
        } catch (Throwable e) {
            log.error("Fail to execute on close consumer on broker listener", e);
        }
    }

    @Override
    public void close() {
        interceptor.close();
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the broker listener class loader", e);
        }
    }
}

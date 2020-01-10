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

import com.google.common.annotations.Beta;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducer;

/**
 * A plugin interface that allows you to listen (and possibly mutate) the
 * client requests to the Pulsar brokers.
 *
 * <p>Exceptions thrown by BrokerEventListener methods will be caught, logged, but
 * not propagated further.
 *
 * <p>BrokerEventListener callbacks may be called from multiple threads. Interceptor
 * implementation must ensure thread-safety, if needed.
 */
@Beta
public interface BrokerEventListener extends AutoCloseable {

    /**
     * This is called when the pulsar client send a new producer request.
     */
    void onNewProducer(CommandProducer command);

    /**
     * This is called when the pulsar client send a subscribe request.
     */
    void onSubscribe(CommandSubscribe command);

    /**
     * This is called when the pulsar client send a unsubscribe request.
     */
    void onUnsubscribe(CommandUnsubscribe command);

    /**
     * This is called when the pulsar client close a producer or the client disconnected.
     */
    void onCloseProducer(CommandCloseProducer command);

    /**
     * This is called when the pulsar client close a consumer or the client disconnected.
     */
    void onCloseConsumer(CommandCloseConsumer command);

    /**
     * Close this broker event listener.
     */
    @Override
    void close();
}

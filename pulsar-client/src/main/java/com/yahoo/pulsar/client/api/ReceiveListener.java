/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.api;

import java.io.Serializable;

/**
 * A listener that will be called in order for every message received.
 *
 *
 */
public interface ReceiveListener extends Serializable {
    /**
     * This method is called whenever a new message is received.
     * 
     * This method will only be called once for each message, unless either application or broker crashes.
     *
     * Application is responsible of handling any exception that could be thrown while processing.
     *
     * @param consumer
     *            the consumer that received a message
     */
    void received(Consumer consumer);
}

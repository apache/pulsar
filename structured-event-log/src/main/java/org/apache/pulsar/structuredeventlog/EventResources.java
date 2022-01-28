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
package org.apache.pulsar.structuredeventlog;

import java.util.function.Supplier;

/**
 * A container for resources
 * The container allows resources to be shared across multiple
 * events with different root events. An example usage of this would be used
 * for a server connection. We want all events initiated on the connection to
 * share resource values, such as remote and local socket. However, each root
 * event on the connection may be for a different trace.
 */
public interface EventResources {
    /**
     * Add a resource for the event. Resources are inherited by
     * child events.
     * @param key the key to identify the resource
     * @param value the value which will be logged for the resource.
     *        This is converted to a string before logging.
     * @return this
     */
    EventResources resource(String key, Object value);

    /**
     * Add a resource for the event using a supplier. The supplier is
     * used in the case that generating the string from the object is
     * expensive or we want to generate a custom string.
     * @param key the key to identify the resource
     * @param value a supplier which returns the value to be logged for
     *              this resource
     * @see #resource(java.lang.String,java.lang.Object)
     */
    EventResources resource(String key, Supplier<String> value);
}

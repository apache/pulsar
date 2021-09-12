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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Structured Logged Event interface.
 *
 * This interface is used to add context information to the log event and eventually log.
 */
public interface Event {
    /**
     * Create a new child event. The child event will inherit the trace ID of the event
     * from which it was created. The child event parentId will be the ID of the event
     * from which it was created.
     * The child will inherit resources from its parent event, but not attributes.
     *
     * @return a new child event
     */
    Event newChildEvent();

    /**
     * Set the trace ID of the event. Normally this will be inherited from the parent
     * event. In the case of a root event, the trace ID will be automatically generated.
     * This method only needs to be used if a traceID has been received from elsewhere,
     * such as from a user request.
     * @param traceId the traceId
     * @return this
     */
    Event traceId(String traceId);

    /**
     * Set the parent ID of the event. Normally this is set automatically for child events.
     * For root events, it is normally empty unless provided by some other means, such
     * as via a user request.
     * @param parentId the parentId
     * @return this
     */
    Event parentId(String parentId);

    /**
     * Mark this event as timed.
     * Timed events will measure the duration between #timed() being called
     * and logged being called. The duration will be in milliseconds.
     *
     * <pre>
     * Event e = logger.newRootEvent().timed();
     * // do something that takes time.
     * e.log(Events.SOME_EVENT);
     * </pre>
     *
     * @return this
     */
    Event timed();

    /**
     * Mark this event as sampled.
     * Sampled events will only log once per specified duration.
     * The sampling key is used to scope the rate limit. All events using the
     * same sampling key will observe the same rate limit.
     * Sampled events are most useful in the data plane, where, if there is an
     * error for one event, there's likely to be a lot of other events which are
     * almost identical.
     *
     * @param samplingKey a key by which to scope the rate limiting
     * @param duration the duration for which one event will be logged
     * @param unit the duration unit
     * @return this
     */
    Event sampled(Object samplingKey, int duration, TimeUnit unit);

    /**
     * Add resources for the event from an EventResources object.
     * @see #resource(java.lang.String,java.lang.Object)
     * @return this
     */
    Event resources(EventResources attrs);

    /**
     * Add a resource for the event. Resources are inherited by
     * child events.
     * @param key the key to identify the resource
     * @param value the value which will be logged for the resource.
     *        This is converted to a string before logging.
     * @return this
     */
    Event resource(String key, Object value);

    /**
     * Add a resource for the event using a supplier. The supplier is
     * used in the case that generating the string from the object is
     * expensive or we want to generate a custom string.
     * @param key the key to identify the resource
     * @param value a supplier which returns the value to be logged for
     *              this resource
     * @see #resource(java.lang.String,java.lang.Object)
     */
    Event resource(String key, Supplier<String> value);

    /**
     * Add an attribute for the event. Attributes are not inherited
     * by child events.
     * @param key the key to identify the attribute
     * @param value the value which will be logged for the attribute.
     *        This is converted to a string, using Object#toString() before logging.
     * @return this
     */
    Event attr(String key, Object value);

    /**
     * Add an attribute for the event using a supplier.
     * @param key the key to identify the attribute
     * @param value a supplier which returns the value to be logged for
     *              this attribute
     * @return this
     */
    Event attr(String key, Supplier<String> value);

    /**
     * Attach an exception to the event.
     * @param t the exception
     * @return this
     */
    Event exception(Throwable t);

    /**
     * Log this event at the error level.
     * @return this
     */
    Event atError();

    /**
     * Log this event at the info level (the default).
     * @return this
     */
    Event atInfo();

    /**
     * Log this event at the warn level.
     * @return this
     */
    Event atWarn();

    /**
     * Log the event, using an enum as the message.
     * Logging with a enum allows documentation and annotations, such as subcomponent
     * to be attached to the message.
     * @param event the event message, in enum form
     */
    void log(Enum<?> event);

    /**
     * Log the event, using a string.
     * @param event the event message.
     */
    void log(String event);

    /**
     * Stash this log event to bridge across an unmodifiable API call.
     * @see StructuredEventLog#unstash()
     */
    void stash();
}

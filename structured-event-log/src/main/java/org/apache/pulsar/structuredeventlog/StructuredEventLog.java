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

import org.apache.pulsar.structuredeventlog.slf4j.Slf4jStructuredEventLog;

/**
 * Structured event logging interface
 *
 * Allows resources and attribute key value pairs to be attached to a logged event.
 *
 * Basic usage:
 * <pre>
 * StructuredEventLog logger = StructuredEventLog.newLogger();
 * logger.newRootEvent()
 *     .resource("remote", remoteAddr)
 *     .resource("local", localAddr)
 *     .attr("path", request.getPath())
 *     .log(Events.READ_REQUEST);
 * </pre>
 */
public interface StructuredEventLog {
    /**
     * Create a new root event. Root events occur in response to some external stimulus, such as
     * a user request, a timer being triggered or a threshold being crossed.
     *
     * The level of the event is INFO by default.
     *
     * The root event will generate a new traceId, and will have a empty parent Id. If this
     * information is provided, as can be the case with a user request, they can be set
     * with Event#traceId(String) and Event#parentId(String).
     */
    Event newRootEvent();

    /**
     * Create an new event resources object, which can be used across multiple
     * root events.
     */
    EventResources newEventResources();

    /**
     * Retrieves an event from the call stack. This can be used, along with Event#stash(),
     * to bridge an event across an API without having to modify the API to pass the event
     * object.
     *
     * For example, the child event, METHOD2, in the following example, will share the traceId
     * with METHOD1, and METHOD1's id will be match the parentId of METHOD2.
     *
     * <pre>
     * void method1() {
     *    Event e = logger.newRootEvent()
     *        .timed()
     *        .resource("foo", bar);
     *
     *    e.stash();
     *    unmodifiableMethod();
     *    e.log(Events.METHOD1);
     * }
     *
     * void unmodifiableMethod() {
     *    logger.unstash().newChildEvent().log(Events.METHOD2);
     * }
     * </pre>
     *
     * This should be used sparingly.
     */
    Event unstash();

    /**
     * Create a new logger object, from which root events can be created.
     */
    public static StructuredEventLog newLogger() {
        return Slf4jStructuredEventLog.INSTANCE;
    }
}

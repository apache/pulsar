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
package org.apache.pulsar.io.core;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * This is an interface for defining BatchSource triggerers. These triggerers trigger
 * the discovery method of the batch source that they are attached to. A BatchSource
 * is configured to use a particular BatchSource Triggerer at the time of job submission.
 *
 * The interface and lifecycle is as follows
 * 1. init - Called after the object is created by reflection.
 *     - The triggerer is created on only one instance of the source job.
 *     - The triggerer is passed its configuration in the init method.
 *     - Trigger also has access to the SourceContext of the BatchSource that it is attached to.
 *       It can use this context to get metadata information about the source as well things like secrets
 *     - This method just inits the triggerer. It doesn't start its execution.
 * 2. start - Is called to actually start the running of the triggerer.
 *     -  Triggerer will use the 'trigger' ConsumerFunction to actually trigger the discovery process
 * 3. stop - Stop from further triggering discovers
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface BatchSourceTriggerer {

    /**
     * initializes the Triggerer with given config. Note that the triggerer doesn't start running
     * until start is called.
     *
     * @param config config needed for triggerer to run
     * @param sourceContext The source context associated with the source
     * The parameter passed to this trigger function is an optional description of the event that caused the trigger
     * @throws Exception throws any exceptions when initializing
     */
    void init(Map<String, Object> config, SourceContext sourceContext) throws Exception;

    /**
     * Triggerer should actually start looking out for trigger conditions.
     *
     * @param trigger The function to be called when its time to trigger the discover
     *                This function can be passed any metadata about this particular
     *                trigger event as its argument
     * This method should return immediately. It is expected that implementations will use their own mechanisms
     *                to schedule the triggers.
     */
    void start(Consumer<String> trigger);

    /**
     * Triggerer should stop triggering.
     *
     */
    void stop();
}
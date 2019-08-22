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

package org.apache.pulsar.functions.runtime;

import org.apache.pulsar.functions.proto.InstanceCommunication;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A function container is an environment for invoking functions.
 */
public interface Runtime {

    void start() throws Exception;

    default void reinitialize() {

    }

    void join() throws Exception;

    void stop() throws Exception;

    default void terminate() throws Exception {
        stop();
    }

    boolean isAlive();

    Throwable getDeathException();

    CompletableFuture<InstanceCommunication.FunctionStatus> getFunctionStatus(int instanceId);

    CompletableFuture<InstanceCommunication.MetricsData> getAndResetMetrics();
    
    CompletableFuture<Void> resetMetrics();
    
    CompletableFuture<InstanceCommunication.MetricsData> getMetrics(int instanceId);

    String getPrometheusMetrics() throws IOException;
}

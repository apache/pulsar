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
package org.apache.pulsar.functions.utils.functions;


import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.functions.ClusterFunctionProducerDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
The purpose of this class is to decouple assignment of WorkerConfig properties from assignment of FunctionDetails
when instantiating the ClusterFunctionProducerDefaults class. We can't inject FunctionDetails into the constructor
of ClusterFunctionProducerDefaults because it's unavailable to WorkerConfig, which is where the cluster defaults
should be validated and provided to ClusterFunctionProducerDefaults. (If we did that, when we perform validation of
cluster function defaults, we'd end up with exceptions being thrown when accessing ClusterFunctionProducerDefaults's
properties, which would bubble up all the way to the client producer. So, we should pass FunctionDetails to
ClusterFunctionProducerDefaults by method injection and a logger to supress function exception throws from occuring when
the producers are created. However, repeatedly passing those parameters via method injection is annoying.
This proxy simplifies those getter calls. Usually, a proxy shares an interface with the class it's proxying, but that
wouldn't be helpful here because the method signatures change to simplify the implementation of its backing class.
 */
public class ClusterFunctionProducerDefaultsProxy {

    private Function.FunctionDetails functionDetails;

    private ClusterFunctionProducerDefaults producerDefaults;

    public ClusterFunctionProducerDefaultsProxy(Function.FunctionDetails functionDetails, ClusterFunctionProducerDefaults producerDefaults) {
        this.functionDetails = functionDetails;
        this.producerDefaults = producerDefaults;
        this.instanceLog = LoggerFactory.getILoggerFactory().getLogger(
                "function-" + functionDetails.getName());
    }

    private Logger instanceLog;
/*
    public boolean getBatchingEnabled() {
        return producerDefaults.getBatchingEnabled(functionDetails, instanceLog);
    }

    public boolean getChunkingEnabled() {
        return producerDefaults.getChunkingEnabled(functionDetails, instanceLog);
    }

    public boolean getBlockIfQueueFull() {
        return producerDefaults.getBlockIfQueueFull(functionDetails, instanceLog);
    }

    public CompressionType getCompressionType() {
        return producerDefaults.getCompressionType(functionDetails, instanceLog);
    }

    public HashingScheme getHashingScheme() {
        return producerDefaults.getHashingScheme(functionDetails, instanceLog);
    }

    public MessageRoutingMode getMessageRoutingMode() {
        return producerDefaults.getMessageRoutingMode(functionDetails, instanceLog);
    }

    public int getBatchingMaxPublishDelay() {
        return producerDefaults.getBatchingMaxPublishDelay(functionDetails, instanceLog);
    }

 */
}

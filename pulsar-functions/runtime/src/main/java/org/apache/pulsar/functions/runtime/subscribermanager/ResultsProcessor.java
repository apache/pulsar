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
/**
 * Spawner is the module responsible for running one particular instance servicing one
 * function. It is responsible for starting/stopping the instance and passing data to the
 * instance and getting the results back.
 */
package org.apache.pulsar.functions.runtime.subscribermanager;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.runtime.container.ExecutionResult;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultsProcessor {
    private static final Logger log = LoggerFactory.getLogger(ResultsProcessor.class);
    private ProducerManager producer;

    public ResultsProcessor(PulsarClient client) {
        producer = new ProducerManager(client);
    }

    public boolean handleResult(FunctionContainer container, ExecutionResult result) {
        if (result.getUserException() != null) {
            log.info("Exception", result.getUserException());
        } else if (result.isTimedOut()) {
            log.info("Timedout");
        } else if (result.getResult() != null && container.getFunctionConfig().getSinkTopic() != null) {
            try {
                producer.publish(container.getFunctionConfig().getSinkTopic(), result.getResult());
            } catch (Exception ex) {
                log.info("Exception while publishing the result", ex);
                throw new RuntimeException(ex);
            }
        }
        return true;
    }
}

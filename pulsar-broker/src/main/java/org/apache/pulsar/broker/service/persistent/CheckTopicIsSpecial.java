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
package org.apache.pulsar.broker.service.persistent;

import java.io.IOError;
import java.io.IOException;
import org.apache.logging.log4j.core.util.Assert;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckTopicIsSpecial {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopic.class);
    public static  boolean checkTopicIsFunctionWorkerService(PersistentTopic topic)  {
        if (!Assert.isNonEmpty(topic)){
            throw new IllegalArgumentException("topic can`t be null");
        }
       WorkerConfig workerConfig =  topic.getBrokerService().getPulsar().getWorkerConfig().get();
        String topicName = topic.getName();
        return workerConfig.getClusterCoordinationTopic().equals(topicName)
                || workerConfig.getFunctionAssignmentTopic().equals(topicName)
                || workerConfig.getFunctionMetadataTopic().equals(topicName);
    }

}

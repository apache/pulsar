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
package org.apache.pulsar.functions.spawner;

import java.util.UUID;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.FunctionID;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.apache.pulsar.functions.runtime.container.SerDe;
import org.apache.pulsar.functions.runtime.container.ThreadFunctionContainerFactory;
import org.apache.pulsar.functions.subscribermanager.SubscriberManager;

public class Spawner {

    public static Spawner createSpawner(FunctionConfig fnConfig,
                                        LimitsConfig limitsConfig,
                                        SerDe serDe,
                                        String pulsarBrokerRootUrl) {
        AssignmentInfo assignmentInfo = new AssignmentInfo(
            fnConfig,
            new FunctionID(),
            UUID.randomUUID().toString(),
            serDe
        );
        return new Spawner(
            limitsConfig,
            assignmentInfo,
            pulsarBrokerRootUrl);
    }

    private LimitsConfig limitsConfig;
    private AssignmentInfo assignmentInfo;
    private String pulsarBrokerRootUrl;
    private ThreadFunctionContainerFactory threadFunctionContainerFactory;
    private FunctionContainer functionContainer;
    private SubscriberManager subscriberManager;

    public Spawner(LimitsConfig limitsConfig, AssignmentInfo assignmentInfo, String pulsarBrokerRootUrl) {
        this.limitsConfig = limitsConfig;
        this.assignmentInfo = assignmentInfo;
        this.pulsarBrokerRootUrl = pulsarBrokerRootUrl;
        this.threadFunctionContainerFactory = new ThreadFunctionContainerFactory();
    }

    public void start() throws Exception {
        subscriberManager = new SubscriberManager(createSubscriptionName(), pulsarBrokerRootUrl);
        functionContainer = threadFunctionContainerFactory.createContainer(createJavaInstanceConfig());
        subscriberManager.addSubscriber(assignmentInfo.getFunctionConfig().getSourceTopic(), functionContainer);
        functionContainer.start();
    }

    public void join() throws Exception {
        functionContainer.join();
    }

    private String createSubscriptionName() {
        return "spawner-" + assignmentInfo.getFunctionConfig().getName() + "-" + assignmentInfo.getFunctionVersion()
                + "-" + assignmentInfo.getFunctionId();
    }

    private JavaInstanceConfig createJavaInstanceConfig() {
        JavaInstanceConfig javaInstanceConfig = new JavaInstanceConfig();
        javaInstanceConfig.setFunctionConfig(assignmentInfo.getFunctionConfig());
        javaInstanceConfig.setFunctionId(assignmentInfo.getFunctionId());
        javaInstanceConfig.setFunctionVersion(assignmentInfo.getFunctionVersion());
        javaInstanceConfig.setSerDe(assignmentInfo.getSerDe());
        javaInstanceConfig.setTimeBudgetInMs(limitsConfig.getTimeBudgetInMs());
        javaInstanceConfig.setMaxMemory(limitsConfig.getMaxMemory());
        return javaInstanceConfig;
    }
}

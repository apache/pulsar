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
package org.apache.pulsar.functions.runtime.spawner;

import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.container.FunctionContainerFactory;
import org.apache.pulsar.functions.runtime.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.FunctionID;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;

@Slf4j
public class Spawner implements AutoCloseable {

    public static Spawner createSpawner(FunctionConfig fnConfig,
                                        LimitsConfig limitsConfig,
                                        String codeFile,
                                        FunctionContainerFactory containerFactory) {
        AssignmentInfo assignmentInfo = new AssignmentInfo(
            fnConfig,
            new FunctionID(),
            UUID.randomUUID().toString()
        );
        return new Spawner(
            limitsConfig,
            assignmentInfo,
            codeFile,
            containerFactory);
    }

    private final LimitsConfig limitsConfig;
    private final AssignmentInfo assignmentInfo;
    private final FunctionContainerFactory threadFunctionContainerFactory;
    private final String codeFile;

    private FunctionContainer functionContainer;

    private Spawner(LimitsConfig limitsConfig,
                    AssignmentInfo assignmentInfo,
                    String codeFile,
                    FunctionContainerFactory containerFactory) {
        this.limitsConfig = limitsConfig;
        this.assignmentInfo = assignmentInfo;
        this.threadFunctionContainerFactory = containerFactory;
        this.codeFile = codeFile;
    }

    public void start() throws Exception {
        log.info("Spawner starting function {}", this.assignmentInfo.getFunctionConfig().getName());
        functionContainer = threadFunctionContainerFactory.createContainer(createJavaInstanceConfig(), codeFile);
        functionContainer.start();
    }

    public void join() throws Exception {
        if (null != functionContainer) {
            functionContainer.join();
        }
    }

    @Override
    public void close() {
        if (null != functionContainer) {
            functionContainer.stop();
        }
    }

    private JavaInstanceConfig createJavaInstanceConfig() {
        JavaInstanceConfig javaInstanceConfig = new JavaInstanceConfig();
        javaInstanceConfig.setFunctionConfig(assignmentInfo.getFunctionConfig());
        javaInstanceConfig.setFunctionId(assignmentInfo.getFunctionId());
        javaInstanceConfig.setFunctionVersion(assignmentInfo.getFunctionVersion());
        javaInstanceConfig.setTimeBudgetInMs(limitsConfig.getMaxTimeMs());
        javaInstanceConfig.setMaxMemory(limitsConfig.getMaxMemoryMb());
        return javaInstanceConfig;
    }
}

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
package org.apache.pulsar.testclient;

import io.netty.util.internal.PlatformDependent;
import lombok.experimental.UtilityClass;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Utility for test clients
 */
@UtilityClass
public class PerfClientUtils {

    private static volatile  Consumer<Integer> exitProcedure = System::exit;

    public static void setExitProcedure(Consumer<Integer> exitProcedure) {
        PerfClientUtils.exitProcedure = Objects.requireNonNull(exitProcedure);
    }

    public static void exit(int code) {
        exitProcedure.accept(code);
    }

    /**
     * Print useful JVM information, you need this information in order to be able
     * to compare the results of executions in different environments.
     * @param log
     */
    public static void printJVMInformation(Logger log) {
        log.info("JVM args {}", ManagementFactory.getRuntimeMXBean().getInputArguments());
        log.info("Netty max memory (PlatformDependent.maxDirectMemory()) {}", FileUtils.byteCountToDisplaySize(PlatformDependent.maxDirectMemory()));
        log.info("JVM max heap memory (Runtime.getRuntime().maxMemory()) {}", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()));
    }


}

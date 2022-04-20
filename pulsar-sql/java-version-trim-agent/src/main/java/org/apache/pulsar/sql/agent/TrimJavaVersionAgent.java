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
package org.apache.pulsar.sql.agent;

import java.lang.instrument.Instrumentation;
import lombok.extern.slf4j.Slf4j;

/**
 * The presto 332 couldn't parse Java version like this `11.0.14.1`,
 * so add java version trim agent to walk around the problem.
 *
 * After the presto upgrade to 332+, we could remove this.
 */
@Slf4j
public class TrimJavaVersionAgent {

    private static final String JAVA_VERSION = "java.version";

    public static String trimJavaVersion(String javaVersion) {
        String[] arr = javaVersion.split("\\.");
        if (arr.length <= 3) {
            return javaVersion;
        }
        return arr[0] + "." + arr[1] + "." + arr[2];
    }

    public static void premain(String agentArgs, Instrumentation inst) {
        String javaVersion = System.getProperty(JAVA_VERSION);
        log.info("original java version => {}", javaVersion);
        String trimVersion = trimJavaVersion(javaVersion);
        log.info("trim java version => {}", javaVersion);
        System.setProperty(JAVA_VERSION, trimVersion);
    }

}

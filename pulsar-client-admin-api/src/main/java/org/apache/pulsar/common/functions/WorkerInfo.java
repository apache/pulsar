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
package org.apache.pulsar.common.functions;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Worker information.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor
@ToString
@Slf4j
public class WorkerInfo {
    private String workerId;
    private String workerHostname;
    private int port;

    public static WorkerInfo of(String workerId, String workerHostname, int port) {
        return new WorkerInfo(workerId, workerHostname, port);
    }

    public static WorkerInfo parseFrom(String str) {
        String[] tokens = str.split(":");
        if (tokens.length != 3) {
            throw new IllegalArgumentException("Invalid string to parse WorkerInfo : " + str);
        }

        String workerId = tokens[0];
        String workerHostname = tokens[1];
        try {
            int port = Integer.parseInt(tokens[2]);

            return new WorkerInfo(workerId, workerHostname, port);
        } catch (NumberFormatException nfe) {
            log.warn("Invalid worker info : {}", str);
            throw nfe;
        }
    }
}

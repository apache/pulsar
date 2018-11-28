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
package org.apache.pulsar.common.policies.data;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class SourceStatus {
    public int numInstances;
    public int numRunning;
    public List<SourceInstanceStatus> instances = new LinkedList<>();

    @Data
    public static class SourceInstanceStatus {
        public int instanceId;
        public SourceInstanceStatusData status;

        @Data
        public static class SourceInstanceStatusData {

            public boolean running;

            public String error;

            public long numRestarts;

            public long numReceived;

            public long numSystemExceptions;

            public List<ExceptionInformation> latestSystemExceptions;

            public long lastInvocationTime;

            public String workerId;
        }
    }

    public void addInstance(SourceInstanceStatus sourceInstanceStatus) {
        instances.add(sourceInstanceStatus);
    }
}

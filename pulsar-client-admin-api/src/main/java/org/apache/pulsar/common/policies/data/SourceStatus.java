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

import java.util.LinkedList;
import java.util.List;
import lombok.Data;

/**
 * Source status.
 */
@Data
public class SourceStatus {
    // The total number of source instances that ought to be running
    public int numInstances;
    // The number of source instances that are actually running
    public int numRunning;
    public List<SourceInstanceStatus> instances = new LinkedList<>();

    /**
     * Source instance status.
     */
    @Data
    public static class SourceInstanceStatus {
        public int instanceId;
        public SourceInstanceStatusData status;

        /**
         * Source instance status data.
         */
        @Data
        public static class SourceInstanceStatusData {
            // Is this instance running?
            public boolean running;

            // Do we have any error while running this instance
            public String error;

            // Number of times this instance has restarted
            public long numRestarts;

            // Number of messages received from source
            public long numReceivedFromSource;

            // Number of times there was a system exception handling messages
            public long numSystemExceptions;

            // A list of the most recent system exceptions
            public List<ExceptionInformation> latestSystemExceptions;

            // Number of times there was a exception from source while reading messages
            public long numSourceExceptions;

            // A list of the most recent source exceptions
            public List<ExceptionInformation> latestSourceExceptions;

            // Number of messages written into pulsar
            public long numWritten;

            // When was the last time we received a message from the source
            public long lastReceivedTime;

            // The worker id on which the source is running
            public String workerId;
        }
    }

    public void addInstance(SourceInstanceStatus sourceInstanceStatus) {
        instances.add(sourceInstanceStatus);
    }

}

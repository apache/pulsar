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
 * Status of Pulsar Sink.
 */
@Data
public class SinkStatus {
    // The total number of sink instances that ought to be running
    public int numInstances;
    // The number of source instances that are actually running
    public int numRunning;
    public List<SinkInstanceStatus> instances = new LinkedList<>();

    /**
     * Status of a Sink instance.
     */
    @Data
    public static class SinkInstanceStatus {
        public int instanceId;
        public SinkInstanceStatusData status;

        /**
         * Status data of a Sink instance.
         */
        @Data
        public static class SinkInstanceStatusData {
            // Is this instance running?
            public boolean running;

            // Do we have any error while running this instance
            public String error;

            // Number of times this instance has restarted
            public long numRestarts;

            // Number of messages read from Pulsar
            public long numReadFromPulsar;

            // Number of times there was a system exception handling messages
            public long numSystemExceptions;

            // A list of the most recent system exceptions
            public List<ExceptionInformation> latestSystemExceptions;

            // Number of times there was a sink exception
            public long numSinkExceptions;

            // A list of the most recent sink exceptions
            public List<ExceptionInformation> latestSinkExceptions;

            // Number of messages written to sink
            public long numWrittenToSink;

            // When was the last time we received a message from Pulsar
            public long lastReceivedTime;

            public String workerId;
        }
    }

    public void addInstance(SinkInstanceStatus sinkInstanceStatus) {
        instances.add(sinkInstanceStatus);
    }

}

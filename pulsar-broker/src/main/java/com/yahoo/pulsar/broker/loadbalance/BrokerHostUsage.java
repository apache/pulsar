/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance;

import java.io.IOException;
import java.io.StringWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.utils.CmdUtility;

/**
 * Class that will return the broker host usage.
 *
 *
 */
public class BrokerHostUsage {
    // The interval for host usage check command
    private final int hostUsageCheckInterval;

    // Path to the pulsar-broker-host-usage script
    private final String usageScriptPath;

    private static final Logger LOG = LoggerFactory.getLogger(BrokerHostUsage.class);

    public BrokerHostUsage(PulsarService pulsar) {
        this.usageScriptPath = pulsar.getConfiguration().getLoadBalancerHostUsageScriptPath();
        this.hostUsageCheckInterval = pulsar.getConfiguration().getLoadBalancerHostUsageCheckIntervalMinutes();
    }

    /**
     * Returns the host usage information in the following format -
     *
     * <pre>
     *    {
     *         "bandwidthIn" : {
     *           "usage" : "100",
     *           "limit" : "1000",
     *        },
     *        "bandwidthOut" : {
     *           "usage" : "659",
     *           "limit" : "1000",
     *        },
     *        "memory" : {
     *           "usage" : "16.0",
     *           "limit" : "16070",
     *       }
     *       "cpu-utilization" : {
     *           "usage"    : "160.0"
     *           "limit"    : "1600"
     *       }
     *     }
     * </pre>
     *
     * @return Broker host usage in the json string format
     *
     * @throws IOException
     */
    public String getBrokerHostUsage() throws IOException {
        StringWriter writer = new StringWriter();
        try {
            /**
             * Spawns a python process and runs the usage exporter script. The script return the machine information in
             * the json format.
             */

            int exitCode = CmdUtility.exec(writer, usageScriptPath, "--host-usage-check-interval",
                    Integer.toString(hostUsageCheckInterval));
            if (exitCode != 0) {
                LOG.warn("Process exited with non-zero exit code - [{}], stderr - [{}] ", exitCode, writer.toString());
                throw new IOException(writer.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Error running the usage script {}", e.getMessage());
            throw e;
        }
        return writer.toString();
    }
}

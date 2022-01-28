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
package org.apache.pulsar.broker.resourcegroup;

import org.apache.pulsar.client.admin.PulsarAdminException;

/*
 * Interface to allow plugging in replacements for resource-quota calculation functions.
 */
public interface ResourceQuotaCalculator {
    /*
     * Determine whether sending the current report of local usage can be suppressed (to reduce network communication).
     *
     * @param currentBytesUsed: the usage in bytes being considered for reporting
     * @param lastReportedBytes: the bytes value that was reported last to all other brokers
     * @param currentMessagesUsed: the usage in messages being considered for reporting
     * @param lastReportedMessages: the messages value that was reported last to all other brokers
     * @param lastReportTimeMSecsSinceEpoch: the time that usage was last reported to all other brokers
     *
     * @return true if the usage must be reported; false if it can be suppressed
     */
    boolean needToReportLocalUsage(long currentBytesUsed, long lastReportedBytes,
                                          long currentMessagesUsed, long lastReportedMessages,
                                          long lastReportTimeMSecsSinceEpoch);

    /*
     * Compute the local quota based on configured values and actual global usage.
     *
     * @param myUsage: the configured limit for usage
     * @param myUsage: local broker's usage in the last period
     * @param allUsages: all brokers' usages in the last period
     *
     * @return local quota value for the next period
     *
     * @throws if the usage is negative
     */
    long computeLocalQuota(long confUsage, long myUsage, long[] allUsages) throws PulsarAdminException;
}

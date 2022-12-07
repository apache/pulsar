/*
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
package org.apache.pulsar.client.api;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Configuration for the close consumer policy.
 *
 * @see ConsumerBuilder#closeWaitForJob(CloseWaitForJobPolicy)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CloseWaitForJobPolicy {

    private boolean closeWaitForJob;
    private int timeout;
    private TimeUnit timeoutUnit;

    private CloseWaitForJobPolicy(boolean closeWaitForJob, int timeout, TimeUnit timeoutUnit) {
        this.closeWaitForJob = closeWaitForJob;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    public boolean isCloseWaitForJob() {
        return closeWaitForJob;
    }

    public int getTimeout() {
        return timeout;
    }

    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    @Override
    public String toString() {
        return "CloseWaitForJobPolicy{"
                + "closeWaitForJob=" + closeWaitForJob
                + ", timeout=" + timeout
                + ", timeoutUnit=" + timeoutUnit
                + '}';
    }

    /**
     * Builder of CloseWaitForJobPolicy.
     */
    public static class Builder {

        private boolean closeWaitForJob;
        private int timeout;
        private TimeUnit timeoutUnit;

        public CloseWaitForJobPolicy.Builder closeWaitForJob(boolean closeWaitForJob) {
            this.closeWaitForJob = closeWaitForJob;
            return this;
        }

        public CloseWaitForJobPolicy.Builder timeout(int timeout, TimeUnit timeoutUnit) {
            this.timeout = timeout;
            this.timeoutUnit = timeoutUnit;
            return this;
        }

        public CloseWaitForJobPolicy build() {
            return new CloseWaitForJobPolicy(closeWaitForJob, timeout, timeoutUnit);
        }
    }

    public static CloseWaitForJobPolicy.Builder builder() {
        return new CloseWaitForJobPolicy.Builder();
    }

    public void verify() {
        if (closeWaitForJob && timeout > 0 && timeoutUnit == null) {
            throw new IllegalArgumentException("Must set timeout unit for timeout.");
        }
    }
}

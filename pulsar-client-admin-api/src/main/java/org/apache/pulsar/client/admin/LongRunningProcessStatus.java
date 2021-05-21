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
package org.apache.pulsar.client.admin;

/**
 * Status of long running process.
 */
public class LongRunningProcessStatus {
    /**
     * Status code.
     */
    public enum Status {
        NOT_RUN,
        RUNNING,
        SUCCESS,
        ERROR
    };

    public Status status;
    public String lastError;

    public LongRunningProcessStatus() {
        this.status = Status.NOT_RUN;
        this.lastError = "";
    }

    LongRunningProcessStatus(Status status, String lastError) {
        this.status = status;
        this.lastError = lastError;
    }

    public static LongRunningProcessStatus forStatus(Status status) {
        return new LongRunningProcessStatus(status, "");
    }

    public static LongRunningProcessStatus forError(String lastError) {
        return new LongRunningProcessStatus(Status.ERROR, lastError);
    }
}

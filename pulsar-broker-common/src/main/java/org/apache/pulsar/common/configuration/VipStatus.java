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
package org.apache.pulsar.common.configuration;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ThreadDumpUtil;

/**
 * Web resource used by the VIP service to check to availability of the service instance.
 */
@Slf4j
@Path("/status.html")
public class VipStatus {

    public static final String ATTRIBUTE_STATUS_FILE_PATH = "statusFilePath";
    public static final String ATTRIBUTE_IS_READY_PROBE = "isReadyProbe";

    // log a full thread dump when a deadlock is detected in status check once every 10 minutes
    // to prevent excessive logging
    private static final long LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED = 600000L;
    private static volatile long threadDumpLoggedTimestamp;

    @Context
    protected ServletContext servletContext;

    @GET
    public String checkStatus() {
        String statusFilePath = (String) servletContext.getAttribute(ATTRIBUTE_STATUS_FILE_PATH);
        @SuppressWarnings("unchecked")
        Supplier<Boolean> isReadyProbe = (Supplier<Boolean>) servletContext.getAttribute(ATTRIBUTE_IS_READY_PROBE);

        boolean isReady = isReadyProbe != null ? isReadyProbe.get() : true;

        if (statusFilePath != null) {
            File statusFile = new File(statusFilePath);
            if (isReady && statusFile.exists() && statusFile.isFile()) {
                // check deadlock
                ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
                long[] threadIds = threadBean.findDeadlockedThreads();
                if (threadIds != null && threadIds.length > 0) {
                    ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, false,
                            false);
                    String threadNames = Arrays.stream(threadInfos)
                            .map(threadInfo -> threadInfo.getThreadName()
                                    + "(tid=" + threadInfo.getThreadId() + ")")
                            .collect(Collectors.joining(", "));
                    if (System.currentTimeMillis() - threadDumpLoggedTimestamp
                            > LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED) {
                        String diagnosticResult = ThreadDumpUtil.buildThreadDiagnosticString();
                        log.error("Deadlock detected, service may be unavailable, "
                                + "thread stack details are as follows: {}.", diagnosticResult);
                        threadDumpLoggedTimestamp = System.currentTimeMillis();
                    } else {
                        log.error("Deadlocked threads detected. {}", threadNames);
                    }
                    throw new WebApplicationException(Status.SERVICE_UNAVAILABLE);
                } else {
                    return "OK";
                }
            }
        }
        log.warn("Failed to access \"status.html\". The service is not ready");
        throw new WebApplicationException(Status.NOT_FOUND);
    }

}

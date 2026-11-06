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
package org.apache.pulsar.tests.performance.launcher;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds why a workload container failed in its log, for the one-line error on the console: the exception the
 * workload failed with, the deepest cause that has a message, and the root cause when it has none, as in
 * {@code IllegalStateException: Cannot restart IoT client, caused by DnsNameResolverException: ... failed to send a
 * query via UDP, caused by StacklessClosedChannelException}. Only the first exception in the log counts.
 */
final class FailureCause {
    // "java.lang.IllegalStateException: message" or "Caused by: java.net.UnknownHostException: message"
    private static final Pattern EXCEPTION = Pattern.compile(
            "^(?:Caused by: )?((?:[a-zA-Z_$][\\w$]*\\.)*([A-Z][\\w$]*(?:Exception|Error|Throwable)))(?::\\s*(.*))?$");

    private FailureCause() {
    }

    /** The failure in {@code log}, or its last line when it has no exception, or null when it is empty. */
    static String of(String log) {
        List<String> lines = log.lines().map(String::strip).filter(line -> !line.isEmpty()).toList();
        String failure = null;
        String cause = null;
        String root = null;
        for (String line : lines) {
            Matcher matcher = EXCEPTION.matcher(line);
            if (!matcher.matches()) {
                continue;
            }
            String described = describe(matcher);
            if (!line.startsWith("Caused by: ")) {
                if (failure != null) {
                    // Another failure, such as one while closing
                    break;
                }
                failure = described;
            } else if (failure != null) {
                root = described;
                if (matcher.group(3) != null && !matcher.group(3).isBlank()) {
                    // Wrappers repeat their cause's message, so the deepest message says most
                    cause = described;
                }
            }
        }
        if (failure == null) {
            return lines.isEmpty() ? null : lines.get(lines.size() - 1);
        }
        StringBuilder result = new StringBuilder(failure);
        if (cause != null) {
            result.append(", caused by ").append(cause);
        }
        if (root != null && !root.equals(cause)) {
            result.append(", caused by ").append(root);
        }
        return result.toString();
    }

    private static String describe(Matcher matcher) {
        String message = matcher.group(3);
        return message == null || message.isBlank() ? matcher.group(2) : matcher.group(2) + ": " + message;
    }
}

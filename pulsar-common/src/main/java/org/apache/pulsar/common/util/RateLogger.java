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

package org.apache.pulsar.common.util;

import org.slf4j.Logger;

import java.util.Objects;

/**
 * This logs the message once in the beginning and once every LOG_INTERVAL.
 *
 * This file borrows some implementations from:
 * @link https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main
 *              /java/org/apache/zookeeper/server/RateLogger.java
 */
public class RateLogger {
    private final long LOG_INTERVAL; // Duration is in ms

    public RateLogger(Logger log) {
        this(log, 100);
    }

    public RateLogger(Logger log, long interval) {
        LOG = log;
        LOG_INTERVAL = interval;
    }

    private final Logger LOG;
    private String msg = null;
    private long timestamp;
    private int count = 0;
    private String value = null;

    public void flush() {
        if (msg != null && count > 0) {
            String log = "";
            if (count > 1) {
                log = "[" + count + " times] ";
            }
            log += "Message: " + msg;
            if (value != null) {
                log += " Last value:" + value;
            }
            LOG.warn(log);
        }
        msg = null;
        value = null;
        count = 0;
    }

    public void rateLimitLog(String newMsg) {
        rateLimitLog(newMsg, null);
    }

    /**
     * In addition to the message, it also takes a value.
     */
    public void rateLimitLog(String newMsg, String newValue) {
        long now = currentElapsedTime();
        if (Objects.equals(newMsg, msg)) {
            ++count;
            value = newValue;
            if (now - timestamp >= LOG_INTERVAL) {
                flush();
                msg = newMsg;
                timestamp = now;
                value = newValue;
            }
        } else {
            flush();
            msg = newMsg;
            value = newValue;
            timestamp = now;
            LOG.warn("Message:{} Value:{}", msg, value);
        }
    }

    /**
     * Returns time in milliseconds as does System.currentTimeMillis(),
     * but uses elapsed time from an arbitrary epoch more like System.nanoTime().
     * The difference is that if somebody changes the system clock,
     * Time.currentElapsedTime will change but nanoTime won't. On the other hand,
     * all of ZK assumes that time is measured in milliseconds.
     * @return The time in milliseconds from some arbitrary point in time.
     */
    public static long currentElapsedTime() {
        return System.nanoTime() / 1000000;
    }
}

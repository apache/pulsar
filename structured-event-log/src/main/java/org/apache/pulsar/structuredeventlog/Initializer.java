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
package org.apache.pulsar.structuredeventlog;

import org.apache.pulsar.structuredeventlog.log4j2.Log4j2StructuredEventLog;
import org.apache.pulsar.structuredeventlog.slf4j.Slf4jStructuredEventLog;

class Initializer {
    static StructuredEventLog get() {
        return INSTANCE;
    }

    private static final StructuredEventLog INSTANCE;

    static {
        StructuredEventLog log = null;
        try {
            // Use Log4j2 if available in the classpath
            Class.forName("org.apache.logging.log4j.LogManager");
            log = Log4j2StructuredEventLog.INSTANCE;
        } catch (Throwable t) {
            // Fallback to Slf4j otherwise
            log = Slf4jStructuredEventLog.INSTANCE;
        }

        INSTANCE = log;
    }
}

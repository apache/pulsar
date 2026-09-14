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
package org.apache.pulsar.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.testng.annotations.Test;

/**
 * Verify that the JUL-to-Log4j2 bridge is active in the test JVM and that
 * log events emitted via {@link java.util.logging} are routed to Log4j2.
 */
public class JulBridgeTest {

    @Test
    public void testJulBridgeIsActive() {
        // The JVM should have been started with
        // -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
        java.util.logging.LogManager logManager = java.util.logging.LogManager.getLogManager();
        assertEquals(logManager.getClass().getName(),
                "org.apache.logging.log4j.jul.LogManager",
                "JUL bridge should be active via -Djava.util.logging.manager system property");
    }

    @Test
    public void testJulLogsAreRoutedToLog4j2() throws Exception {
        String loggerName = "org.apache.pulsar.test.jul.bridge";

        // Attach a TestLogAppender to capture log events for this logger
        try (TestLogAppender appender = TestLogAppender.create(Optional.of(loggerName))) {
            // Log via java.util.logging API
            Logger julLogger = Logger.getLogger(loggerName);
            julLogger.severe("JUL SEVERE test message");
            julLogger.warning("JUL WARNING test message");
            julLogger.info("JUL INFO test message");

            // Verify that the JUL log events were routed to Log4j2
            assertTrue(appender.getEvents().stream()
                            .anyMatch(e -> e.getMessage().getFormattedMessage().contains("JUL SEVERE test message")
                                    && e.getLevel() == org.apache.logging.log4j.Level.ERROR),
                    "JUL SEVERE should be routed to Log4j2 as ERROR");

            assertTrue(appender.getEvents().stream()
                            .anyMatch(e -> e.getMessage().getFormattedMessage().contains("JUL WARNING test message")
                                    && e.getLevel() == org.apache.logging.log4j.Level.WARN),
                    "JUL WARNING should be routed to Log4j2 as WARN");

            assertTrue(appender.getEvents().stream()
                            .anyMatch(e -> e.getMessage().getFormattedMessage().contains("JUL INFO test message")
                                    && e.getLevel() == org.apache.logging.log4j.Level.INFO),
                    "JUL INFO should be routed to Log4j2 as INFO");
        }
    }

    @Test
    public void testJulExceptionIsPreserved() throws Exception {
        String loggerName = "org.apache.pulsar.test.jul.exception";

        try (TestLogAppender appender = TestLogAppender.create(Optional.of(loggerName))) {
            Logger julLogger = Logger.getLogger(loggerName);
            RuntimeException testException = new RuntimeException("test exception from JUL");
            julLogger.log(Level.SEVERE, "Error occurred", testException);

            // Verify that the exception is preserved in the Log4j2 event
            Optional<LogEvent> event = appender.getEvents().stream()
                    .filter(e -> e.getMessage().getFormattedMessage().contains("Error occurred"))
                    .findFirst();
            assertTrue(event.isPresent(), "Should find the log event");
            assertTrue(event.get().getThrown() instanceof RuntimeException,
                    "Exception should be preserved");
            assertEquals(event.get().getThrown().getMessage(), "test exception from JUL");
        }
    }
}

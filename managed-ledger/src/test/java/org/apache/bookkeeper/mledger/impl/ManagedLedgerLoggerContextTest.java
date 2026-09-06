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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.github.merlimat.slog.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.testng.annotations.Test;

/**
 * Verifies that the logger context attributes set via {@link ManagedLedgerConfig#setLoggerContext(Logger)} are
 * inherited by the managed ledger and cursor loggers.
 */
public class ManagedLedgerLoggerContextTest extends MockedBookKeeperTestCase {

    private static class CapturingAppender extends AbstractAppender {
        private final List<Map<String, String>> contextData = new ArrayList<>();
        private final List<String> messages = new ArrayList<>();

        CapturingAppender() {
            super("ml-logger-context-capture", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public synchronized void append(LogEvent event) {
            // The event is a reusable mutable instance: snapshot the data
            Map<String, String> attrs = new HashMap<>();
            event.getContextData().forEach((k, v) -> attrs.put(k, String.valueOf(v)));
            contextData.add(attrs);
            messages.add(event.getMessage().getFormattedMessage());
        }

        synchronized Map<String, String> attrsForMessage(String message) {
            for (int i = 0; i < messages.size(); i++) {
                if (messages.get(i).equals(message)) {
                    return contextData.get(i);
                }
            }
            return null;
        }
    }

    @Test
    public void testLoggerContextInheritance() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        loggerContext.getConfiguration().getRootLogger().addAppender(appender, Level.INFO, null);
        loggerContext.updateLoggers();

        try {
            ManagedLedgerConfig config = new ManagedLedgerConfig();
            config.setLoggerContext(Logger.get(getClass())
                    .with()
                    .attr("topic", "persistent://public/default/my-topic")
                    .attr("segment", "0000-7fff-1")
                    .build());

            ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("logger_context_ledger", config);
            ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub-1");

            ledger.getLogger().info("ml-logger-probe");
            cursor.log.info("cursor-logger-probe");

            Map<String, String> mlAttrs = appender.attrsForMessage("ml-logger-probe");
            assertTrue(mlAttrs != null, "Expected the managed ledger probe event to be captured");
            assertEquals(mlAttrs.get("topic"), "persistent://public/default/my-topic");
            assertEquals(mlAttrs.get("segment"), "0000-7fff-1");
            assertEquals(mlAttrs.get("managedLedger"), "logger_context_ledger");

            Map<String, String> cursorAttrs = appender.attrsForMessage("cursor-logger-probe");
            assertTrue(cursorAttrs != null, "Expected the cursor probe event to be captured");
            assertEquals(cursorAttrs.get("topic"), "persistent://public/default/my-topic");
            assertEquals(cursorAttrs.get("segment"), "0000-7fff-1");
            assertEquals(cursorAttrs.get("managedLedger"), "logger_context_ledger");
            assertEquals(cursorAttrs.get("cursor"), "sub-1");
        } finally {
            loggerContext.getConfiguration().getRootLogger().removeAppender(appender.getName());
            loggerContext.updateLoggers();
            appender.stop();
        }
    }
}

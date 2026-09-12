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
package org.apache.pulsar.broker.authorization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Captures the Log4j2 events emitted by a single logger, so that tests can assert on the level a
 * given condition is reported at. slog resolves to its Log4j2 backend whenever log4j-core is on the
 * classpath, so this captures slog output too.
 */
final class LogCapture extends AbstractAppender implements AutoCloseable {

    private static final AtomicInteger ID = new AtomicInteger();

    private final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
    private final String loggerName;
    private final LoggerConfig loggerConfig;
    private final LoggerContext context;

    /**
     * Starts capturing the events logged by {@code loggerOwner}'s logger. The capture is released on
     * {@link #close()}.
     */
    static LogCapture attach(Class<?> loggerOwner) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig(loggerOwner.getName());
        LogCapture capture = new LogCapture(loggerOwner.getName(), loggerConfig, context);
        capture.start();
        // The resolved LoggerConfig is often an ancestor (usually root), so this appender may also see
        // events from unrelated loggers. append() filters them out by name.
        loggerConfig.addAppender(capture, Level.ALL, null);
        context.updateLoggers();
        return capture;
    }

    private LogCapture(String loggerName, LoggerConfig loggerConfig, LoggerContext context) {
        super("LogCapture-" + ID.incrementAndGet(), null, PatternLayout.createDefaultLayout(), false, null);
        this.loggerName = loggerName;
        this.loggerConfig = loggerConfig;
        this.context = context;
    }

    @Override
    public void append(LogEvent event) {
        if (loggerName.equals(event.getLoggerName())) {
            events.add(event.toImmutable());
        }
    }

    /** Returns the formatted messages captured at {@code level}, in the order they were logged. */
    List<String> messagesAt(Level level) {
        synchronized (events) {
            return events.stream()
                    .filter(event -> event.getLevel() == level)
                    .map(event -> event.getMessage().getFormattedMessage())
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void close() {
        stop();
        loggerConfig.removeAppender(getName());
        context.updateLoggers();
    }
}

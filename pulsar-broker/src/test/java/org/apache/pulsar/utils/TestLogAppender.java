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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Log4J appender that captures all log events for a specified logger.
 */
public class TestLogAppender extends AbstractAppender implements AutoCloseable {
    private final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
    private static AtomicInteger idGenerator = new AtomicInteger(0);
    private final LoggerConfig loggerConfig;
    private final Runnable onConfigurationChange;

    /**
     * Create a new TestLogAppender. Use the {@link #close()} method to stop it and unregister it from Log4J.
     * @param loggerName the logger name to register to. Pass Optional.empty() to register to the root logger.
     * @return return the new TestLogAppender instance.
     */
    public static TestLogAppender create(Optional<String> loggerName) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();
        LoggerConfig loggerConfig = loggerName.map(config::getLoggerConfig).orElseGet(config::getRootLogger);
        TestLogAppender testAppender = new TestLogAppender(loggerConfig, context::updateLoggers);
        testAppender.start();
        loggerConfig.addAppender(testAppender, Level.ALL, null);
        context.updateLoggers();
        return testAppender;
    }

    TestLogAppender(LoggerConfig loggerConfig, Runnable onConfigurationChange) {
        super("TestAppender" + idGenerator.incrementAndGet(), null, PatternLayout.createDefaultLayout(), false, null);
        this.loggerConfig = loggerConfig;
        this.onConfigurationChange = onConfigurationChange;
    }

    @Override
    public void append(LogEvent event) {
        events.add(event.toImmutable());
    }

    public List<LogEvent> getEvents() {
        return new ArrayList<>(events);
    }

    public void clearEvents() {
        events.clear();
    }

    @Override
    public void close() throws Exception {
        stop(1, TimeUnit.SECONDS);
    }

    @Override
    protected boolean stop(long timeout, TimeUnit timeUnit, boolean changeLifeCycleState) {
        boolean stopped = super.stop(timeout, timeUnit, changeLifeCycleState);
        loggerConfig.removeAppender(getName());
        onConfigurationChange.run();
        return stopped;
    }
}

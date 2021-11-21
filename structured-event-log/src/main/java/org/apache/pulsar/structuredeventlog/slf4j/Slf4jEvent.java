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
package org.apache.pulsar.structuredeventlog.slf4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.pulsar.structuredeventlog.Event;
import org.apache.pulsar.structuredeventlog.EventGroup;
import org.apache.pulsar.structuredeventlog.EventResources;
import org.apache.pulsar.structuredeventlog.EventResourcesImpl;

import org.slf4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Slf4jEvent implements Event {
    private static final ThreadLocal<Map<Object, Logger>> loggersTLS = ThreadLocal.withInitial(() -> new HashMap<>());
    private static final Logger stringLogger = LoggerFactory.getLogger("stevlog");

    private final String id;
    private final Clock clock;
    private String traceId = null;
    private String parentId = null;
    private List<Object> attributes = null;
    private Level level = Level.INFO;
    private Throwable throwable = null;
    private Instant startTime = null;
    private final EventResourcesImpl resources;

    Slf4jEvent(Clock clock, EventResourcesImpl parentResources) {
        this.id = randomId();
        this.clock = clock;
        this.resources = new EventResourcesImpl(parentResources);
    }

    @Override
    public Event newChildEvent() {
        return new Slf4jEvent(clock, resources).traceId(traceId).parentId(id);
    }

    @Override
    public Event traceId(String traceId) {
        this.traceId = traceId;
        return this;
    }

    @Override
    public Event parentId(String parentId) {
        this.parentId = parentId;
        return this;
    }

    @Override
    public Event timed() {
        startTime = clock.instant();
        return this;
    }

    @Override
    public Event sampled(Object samplingKey, int duration, TimeUnit unit) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Event resources(EventResources other) {
        if (other instanceof EventResourcesImpl) {
            this.resources.copyFrom((EventResourcesImpl)other);
        }
        return this;
    }

    @Override
    public Event resource(String key, Object value) {
        resources.resource(key, value);
        return this;
    }

    @Override
    public Event resource(String key, Supplier<String> value) {
        resources.resource(key, value);
        return this;
    }

    @Override
    public Event attr(String key, Object value) {
        getAttributes().add(key);
        getAttributes().add(value);
        return this;
    }

    @Override
    public Event attr(String key, Supplier<String> value) {
        this.attr(key, (Object)value);
        return this;
    }

    @Override
    public Event exception(Throwable t) {
        this.throwable = t;
        return this;
    }

    @Override
    public Event atError() {
        this.level = Level.ERROR;
        return this;
    }

    @Override
    public Event atInfo() {
        this.level = Level.INFO;
        return this;
    }

    @Override
    public Event atWarn() {
        this.level = Level.WARN;
        return this;
    }

    @Override
    public void log(Enum<?> event) {
        EventGroup g = event.getClass().getAnnotation(EventGroup.class);
        Map<Object, Logger> loggers = loggersTLS.get();
        Logger logger;
        if (g != null) {
            String component = g.component();
            MDC.put("component", component);
            // do get, then compute to avoid alloc for compute supplier
            logger = loggers.get(event);
            if (logger == null) {
                logger = loggers.compute(event,
                        (k, v) -> LoggerFactory.getLogger(
                                new StringBuilder("stevlog.").append(component)
                                .append(".").append(event).toString()));
            }
        } else {
            logger = loggers.get(event);
            if (logger == null) {
                logger = loggers.compute(event,
                        (k, v) -> LoggerFactory.getLogger(
                                new StringBuilder("stevlog.").append(event).toString()));
            }
        }
        logInternal(logger, event.toString());
    }

    @Override
    public void log(String event) {
        logInternal(stringLogger, event);
    }

    private void logInternal(Logger logger, String event) {
        try {
            MDC.put("id", id);
            if (traceId != null) {
                MDC.put("traceId", traceId);
            }
            if (parentId != null) {
                MDC.put("parentId", parentId);
            }
            resources.forEach(MDC::put);
            if (attributes != null) {
                EventResourcesImpl.forEach(attributes, MDC::put);
            }
            if (startTime != null) {
                MDC.put("startTimestamp", startTime.toString());
                MDC.put("durationMs", String.valueOf(Duration.between(startTime, clock.instant()).toMillis()));
            }
            switch (level) {
            case ERROR:
                if (throwable != null) {
                    logger.error(event, throwable);
                } else {
                    logger.error(event);
                }
                break;
            case WARN:
                if (throwable != null) {
                    logger.warn(event, throwable);
                } else {
                    logger.warn(event);
                }
                break;
            case INFO:
            default:
                if (throwable != null) {
                    logger.info(event, throwable);
                } else {
                    logger.info(event);
                }
                break;
            }
        } finally {
            MDC.clear();
        }
    }

    @Override
    public void stash() {
        throw new UnsupportedOperationException("TODO");
    }

    private List<Object> getAttributes() {
        if (attributes == null) {
            attributes = new ArrayList<>();
        }
        return attributes;
    }

    static String randomId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(0x100000000000000L,
                                                     0xFFFFFFFFFFFFFFFL),
                16);
    }

    enum Level {
        INFO,
        WARN,
        ERROR
    }
}

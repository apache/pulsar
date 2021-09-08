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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig ;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.pulsar.structuredeventlog.Event;
import org.apache.pulsar.structuredeventlog.EventGroup;
import org.apache.pulsar.structuredeventlog.EventResources;
import org.apache.pulsar.structuredeventlog.StructuredEventLog;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StructuredEventLogTest {
    private final static String APPENDER_NAME = "stevlogtest";
    StringWriter writer;

    @BeforeMethod
    public void setupLog4j() throws Exception {
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();

        writer = new StringWriter();


        LoggerConfig logConfig = config.getLoggerConfig(APPENDER_NAME);
        for (Map.Entry<String, Appender> a : logConfig.getAppenders().entrySet()) {
            if (a.getKey().equals(APPENDER_NAME)) {
                a.getValue().stop();
            }
        }
        logConfig.removeAppender(APPENDER_NAME);

        JsonLayout layout = JsonLayout.newBuilder().setEventEol(true).setCompact(true).setProperties(true).build();
        Appender appender = WriterAppender.createAppender(layout, null, writer, "stevlogtest", false, true);
        appender.start();
        logConfig.addAppender(appender, null, null);
        logConfig.setLevel(Level.DEBUG);
    }

    @Test
    public void testTraceId() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        Event e = log.newRootEvent();
        e.newChildEvent().log("child");
        e.log("parent");

        log.newRootEvent().log("second");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("child"));
        assertThat(logged.get(1).get("message"), equalTo("parent"));
        assertThat(logged.get(2).get("message"), equalTo("second"));

        assertThat(contextMapField(logged.get(0), "traceId"),
                   equalTo(contextMapField(logged.get(1), "traceId")));
        assertThat(contextMapField(logged.get(0), "traceId"),
                   not(equalTo(contextMapField(logged.get(2), "traceId"))));
    }

    @Test
    public void testParentId() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        Event e1 = log.newRootEvent();
        Event e2 = e1.newChildEvent();
        e2.newChildEvent().log("child2");
        e2.log("child1");
        e1.log("parent");

        log.newRootEvent().log("second");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("child2"));
        assertThat(logged.get(1).get("message"), equalTo("child1"));
        assertThat(logged.get(2).get("message"), equalTo("parent"));
        assertThat(logged.get(3).get("message"), equalTo("second"));

        assertThat(contextMapField(logged.get(0), "parentId"), not(nullValue()));
        assertThat(contextMapField(logged.get(0), "parentId"),
                   equalTo(contextMapField(logged.get(1), "id")));
        assertThat(contextMapField(logged.get(1), "parentId"),
                   equalTo(contextMapField(logged.get(2), "id")));
        assertThat(contextMapField(logged.get(2), "parentId"), nullValue());
        assertThat(contextMapField(logged.get(3), "parentId"), nullValue());
    }

    @Test
    public void testResources() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        EventResources res = log.newEventResources()
            .resource("r1", "v1")
            .resource("r2", () -> "v2");
        Event e1 = log.newRootEvent()
            .resources(res)
            .resource("r3", "v3")
            .resource("r4", () -> "v4");
        Event e2 = e1.newChildEvent().resource("r5", "v5");
        e2.newChildEvent().resource("r6", "v6").log("child2");
        e2.log("child1");
        e1.log("parent");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("child2"));
        assertThat(logged.get(1).get("message"), equalTo("child1"));
        assertThat(logged.get(2).get("message"), equalTo("parent"));

        assertThat(contextMapField(logged.get(0), "r6"), equalTo("v6"));
        assertThat(contextMapField(logged.get(0), "r5"), equalTo("v5"));
        assertThat(contextMapField(logged.get(0), "r4"), equalTo("v4"));
        assertThat(contextMapField(logged.get(0), "r3"), equalTo("v3"));
        assertThat(contextMapField(logged.get(0), "r2"), equalTo("v2"));
        assertThat(contextMapField(logged.get(0), "r1"), equalTo("v1"));

        assertThat(contextMapField(logged.get(1), "r6"), nullValue());
        assertThat(contextMapField(logged.get(1), "r5"), equalTo("v5"));
        assertThat(contextMapField(logged.get(1), "r4"), equalTo("v4"));
        assertThat(contextMapField(logged.get(1), "r3"), equalTo("v3"));
        assertThat(contextMapField(logged.get(1), "r2"), equalTo("v2"));
        assertThat(contextMapField(logged.get(1), "r1"), equalTo("v1"));

        assertThat(contextMapField(logged.get(2), "r6"), nullValue());
        assertThat(contextMapField(logged.get(2), "r5"), nullValue());
        assertThat(contextMapField(logged.get(2), "r4"), equalTo("v4"));
        assertThat(contextMapField(logged.get(2), "r3"), equalTo("v3"));
        assertThat(contextMapField(logged.get(2), "r2"), equalTo("v2"));
        assertThat(contextMapField(logged.get(2), "r1"), equalTo("v1"));
    }

    @Test
    public void testResourcesNullTest() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        EventResources res = log.newEventResources()
            .resource(null, "v1")
            .resource("r1", null)
            .resource("r2", () -> null);
        Event e1 = log.newRootEvent()
            .resources(res)
            .resource(null, "v2")
            .resource("r3", null)
            .resource("r4", () -> null);
        e1.newChildEvent()
            .resource(null, "v3")
            .resource("r5", null)
            .log("child1");
        e1.log("parent");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("child1"));
        assertThat(logged.get(1).get("message"), equalTo("parent"));

        assertThat(contextMapField(logged.get(0), "r5"), equalTo("null"));
        assertThat(contextMapField(logged.get(0), "r4"), equalTo("null"));
        assertThat(contextMapField(logged.get(0), "r3"), equalTo("null"));
        assertThat(contextMapField(logged.get(0), "r2"), equalTo("null"));
        assertThat(contextMapField(logged.get(0), "r1"), equalTo("null"));
        assertThat(contextMapField(logged.get(0), "null"), equalTo("v3"));

        assertThat(contextMapField(logged.get(1), "r5"), nullValue());
        assertThat(contextMapField(logged.get(1), "r4"), equalTo("null"));
        assertThat(contextMapField(logged.get(1), "r3"), equalTo("null"));
        assertThat(contextMapField(logged.get(1), "r2"), equalTo("null"));
        assertThat(contextMapField(logged.get(1), "r1"), equalTo("null"));
        assertThat(contextMapField(logged.get(1), "null"), equalTo("v2"));
    }

    @Test
    public void testAttributes() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        Event e1 = log.newRootEvent()
            .attr("a1", "v1")
            .attr("a2", () -> "v2");
        Event e2 = e1.newChildEvent().attr("a3", "v3");
        e2.newChildEvent().resource("a4", "v4").log("child2");
        e2.log("child1");
        e1.log("parent");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("child2"));
        assertThat(logged.get(1).get("message"), equalTo("child1"));
        assertThat(logged.get(2).get("message"), equalTo("parent"));

        assertThat(contextMapField(logged.get(0), "a4"), equalTo("v4"));
        assertThat(contextMapField(logged.get(0), "a3"), nullValue());
        assertThat(contextMapField(logged.get(0), "a2"), nullValue());
        assertThat(contextMapField(logged.get(0), "a1"), nullValue());

        assertThat(contextMapField(logged.get(1), "a4"), nullValue());
        assertThat(contextMapField(logged.get(1), "a3"), equalTo("v3"));
        assertThat(contextMapField(logged.get(1), "a2"), nullValue());
        assertThat(contextMapField(logged.get(1), "a1"), nullValue());

        assertThat(contextMapField(logged.get(2), "a4"), nullValue());
        assertThat(contextMapField(logged.get(2), "a3"), nullValue());
        assertThat(contextMapField(logged.get(2), "a2"), equalTo("v2"));
        assertThat(contextMapField(logged.get(2), "a1"), equalTo("v1"));
    }

    @Test
    public void testAttributedNullTest() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        log.newRootEvent()
            .attr(null, "v1")
            .attr("a1", null)
            .attr("a2", () -> null)
            .log("msg");

        log.newRootEvent()
            .attr(null, "v1")
            .attr("a1", null)
            .attr("a2", () -> null)
            .log("msg");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("msg"));

        assertThat(contextMapField(logged.get(0), "null"), equalTo("v1"));
        assertThat(contextMapField(logged.get(0), "a1"), equalTo("null"));
        assertThat(contextMapField(logged.get(0), "a2"), equalTo("null"));
    }

    @Test
    public void testInfoLevel() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        log.newRootEvent().log("info1");
        log.newRootEvent().atInfo().log("info2");
        log.newRootEvent().atError().atInfo().log("info3");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("info1"));
        assertThat(logged.get(1).get("message"), equalTo("info2"));
        assertThat(logged.get(2).get("message"), equalTo("info3"));

        assertThat(logged.get(0).get("level"), equalTo("INFO"));
        assertThat(logged.get(1).get("level"), equalTo("INFO"));
        assertThat(logged.get(2).get("level"), equalTo("INFO"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInfoLevelException() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        log.newRootEvent().exception(new Throwable("cause1")).log("info1");
        log.newRootEvent().atInfo().exception(new Throwable("cause2")).log("info2");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("info1"));

        assertThat(((Map<String, Object>)logged.get(0).get("thrown")).get("message"), equalTo("cause1"));
        assertThat(logged.get(1).get("message"), equalTo("info2"));
        assertThat(((Map<String, Object>)logged.get(1).get("thrown")).get("message"), equalTo("cause2"));
    }

    @Test
    public void testWarnLevel() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        log.newRootEvent().atWarn().log("warn1");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("warn1"));
        assertThat(logged.get(0).get("level"), equalTo("WARN"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWarnLevelException() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        log.newRootEvent().atWarn().exception(new Throwable("cause1")).log("warn1");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("warn1"));
        assertThat(((Map<String, Object>)logged.get(0).get("thrown")).get("message"), equalTo("cause1"));
    }

    @Test
    public void testErrorLevel() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        log.newRootEvent().atError().log("error1");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("error1"));
        assertThat(logged.get(0).get("level"), equalTo("ERROR"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testErrorLevelException() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();

        log.newRootEvent().atError().exception(new Throwable("cause1")).log("error1");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("error1"));
        assertThat(((Map<String, Object>)logged.get(0).get("thrown")).get("message"), equalTo("cause1"));
    }


    @Test
    public void testTimedEvent() throws Exception {
        MockClock clock = new MockClock();
        StructuredEventLog log = StructuredEventLog.newLogger();
        ((Slf4jStructuredEventLog)log).clock = clock;
        Event e = log.newRootEvent().timed();
        clock.advanceTime(1234, TimeUnit.MILLISECONDS);
        e.log("timed");

        List<Map<String, Object>> logged = getLogged();
        assertThat(logged.get(0).get("message"), equalTo("timed"));
        assertThat(contextMapField(logged.get(0), "startTimestamp"), equalTo("1970-01-02T03:46:40Z"));
        assertThat(contextMapField(logged.get(0), "durationMs"), equalTo("1234"));
    }

    @EventGroup(component="foobar")
    public enum Events {
        TEST_EVENT
    }

    @Test
    public void testEventGroups() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();
        log.newRootEvent().log(Events.TEST_EVENT);

        List<Map<String, Object>> logged = getLogged();
        System.out.println(logged);
        assertThat(logged.get(0).get("message"), equalTo("TEST_EVENT"));
        assertThat(logged.get(0).get("loggerName"), equalTo("stevlog.foobar.TEST_EVENT"));
        assertThat(contextMapField(logged.get(0), "component"), equalTo("foobar"));
    }

    public enum BareEvents {
        BARE_EVENT
    }

    @Test
    public void testBareEnum() throws Exception {
        StructuredEventLog log = StructuredEventLog.newLogger();
        log.newRootEvent().log(BareEvents.BARE_EVENT);

        List<Map<String, Object>> logged = getLogged();
        System.out.println(logged);
        assertThat(logged.get(0).get("message"), equalTo("BARE_EVENT"));
        assertThat(logged.get(0).get("loggerName"), equalTo("stevlog.BARE_EVENT"));
        assertThat(contextMapField(logged.get(0), "component"), nullValue());
    }

    @SuppressWarnings("unchecked")
    private Object contextMapField(Map<String, Object> map, String field) {
        return ((Map<String, Object>)map.get("contextMap")).get(field);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getLogged() throws Exception {
        List<Map<String, Object>> logged = new ArrayList<>();
        ObjectMapper o = new ObjectMapper();
        try (BufferedReader r = new BufferedReader(new StringReader(writer.toString()))) {
            String line = r.readLine();
            while (line != null) {
                Map<String, Object> log = o.readValue(line, Map.class);
                if (log.get("loggerName").toString().startsWith("stevlog")) {
                    logged.add(log);
                }
                line = r.readLine();
            }
        }
        return logged;
    }

    private static class MockClock extends Clock {
        AtomicLong ticker = new AtomicLong(100000000);

        void advanceTime(int duration, TimeUnit unit) {
            ticker.addAndGet(unit.toMillis(duration));
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(ticker.get());
        }

        @Override
        public ZoneId getZone( ) {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zoneId) {
            return this;
        }
    }
}


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
package org.apache.pulsar.functions.composition.windowing;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.utils.Utf8StringSerDe;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;


/**
 * Unit tests for {@link WindowedPulsarFunction}
 */
@Slf4j
public class WindowedPulsarFunctionTest {

    private static class TestWindowedPulsarFunction extends WindowedPulsarFunction<Long, Long> {

        List<Window<Long>> windows = new ArrayList<>();

        @Override
        public Long handleRequest(Window<Long> inputWindow, WindowContext context) throws Exception {
            windows.add(inputWindow);
            return null;
        }
    }

    private static class TestTimestampExtractor implements TimestampExtractor<Long> {
        @Override
        public long extractTimestamp(Long input) {
            return input;
        }
    }

    private static class TestWrongTimestampExtractor implements TimestampExtractor<String> {
        @Override
        public long extractTimestamp(String input) {
            return Long.valueOf(input);
        }
    }


    private TestWindowedPulsarFunction testWindowedPulsarFunction;
    private Context context;

    @BeforeMethod
    public void setUp() {
        testWindowedPulsarFunction = new TestWindowedPulsarFunction();
        context = Mockito.mock(Context.class);
        Mockito.doReturn("test-function").when(context).getFunctionName();
        Mockito.doReturn("test-namespace").when(context).getNamespace();
        Mockito.doReturn("test-tenant").when(context).getTenant();
        Mockito.doReturn(TestTimestampExtractor.class.getName()).when(context).getUserConfigValue
                ("timestampExtractorClassName");
        Mockito.doReturn("20").when(context).getUserConfigValue("windowLengthDurationMs");
        Mockito.doReturn("10").when(context).getUserConfigValue("slidingIntervalDurationMs");
        Mockito.doReturn("5").when(context).getUserConfigValue("maxLagMs");
        Mockito.doReturn("test-source-topic").when(context).getTopicName();
        Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
        Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
        Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
        // trigger manually to avoid timing issues
        Mockito.doReturn("100000").when(context).getUserConfigValue("watermarkEmitIntervalMs");
    }

    @AfterMethod
    public void cleanUp() {
        testWindowedPulsarFunction.shutdown();
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testExecuteWithWrongType() throws Exception {
        Mockito.doReturn(TestWrongTimestampExtractor.class.getName()).when(context).getUserConfigValue
                ("timestampExtractorClassName");
        testWindowedPulsarFunction.process(10L, context);
    }

    @Test
    public void testExecuteWithTs() throws Exception {
        long[] timestamps = {603, 605, 607, 618, 626, 636};
        for (long ts : timestamps) {
            testWindowedPulsarFunction.process(ts, context);
        }
        testWindowedPulsarFunction.waterMarkEventGenerator.run();
        assertEquals(3, testWindowedPulsarFunction.windows.size());
        Window<Long> first = testWindowedPulsarFunction.windows.get(0);
        assertArrayEquals(
                new long[]{603, 605, 607},
                new long[]{first.get().get(0), first.get().get(1), first.get().get(2)});

        Window<Long> second = testWindowedPulsarFunction.windows.get(1);
        assertArrayEquals(
                new long[]{603, 605, 607, 618},
                new long[]{second.get().get(0), second.get().get(1), second.get().get(2), second.get().get(3)});

        Window<Long> third = testWindowedPulsarFunction.windows.get(2);
        assertArrayEquals(new long[]{618, 626}, new long[]{third.get().get(0), third.get().get(1)});
    }

    @Test
    public void testPrepareLateTupleStreamWithoutTs() throws Exception {
        context = Mockito.mock(Context.class);
        Mockito.doReturn("test-function").when(context).getFunctionName();
        Mockito.doReturn("test-namespace").when(context).getNamespace();
        Mockito.doReturn("test-tenant").when(context).getTenant();
        Mockito.doReturn("test-source-topic").when(context).getTopicName();
        Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
        Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
        Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
        Mockito.doReturn("20").when(context).getUserConfigValue("windowLengthDurationMs");
        Mockito.doReturn("10").when(context).getUserConfigValue("slidingIntervalDurationMs");
        Mockito.doReturn("$late").when(context).getUserConfigValue("lateDataTopic");
        Mockito.doReturn("5").when(context).getUserConfigValue("maxLagMs");
        Mockito.doReturn("10").when(context).getUserConfigValue("watermarkEmitIntervalMs");

        try {
            testWindowedPulsarFunction.process(10L, context);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Late data topic can be defined only when specifying a "
                    + "timestamp extractor class");
        }
    }

    @Test
    public void testExecuteWithLateTupleStream() throws Exception {
        Mockito.doReturn("$late").when(context).getUserConfigValue("lateDataTopic");

        long[] timestamps = {603, 605, 607, 618, 626, 636, 600};
        List<Long> events = new ArrayList<>(timestamps.length);

        for (long ts : timestamps) {
            events.add(ts);
            testWindowedPulsarFunction.process(ts, context);

            //Update the watermark to this timestamp
            testWindowedPulsarFunction.waterMarkEventGenerator.run();
        }
        System.out.println(testWindowedPulsarFunction.windows);
        long event = events.get(events.size() - 1);
        Mockito.verify(context).publish("$late", event, Utf8StringSerDe.class.getName());
    }

    @Test
    public void testSettingSlidingCountWindow() throws Exception {
        final Object[][] args = new Object[][]{
                {-1, 10},
                {10, -1},
                {0, 10},
                {10, 0},
                {0, 0},
                {-1, -1},
                {5, 10},
                {1, 1},
                {10, 5},
                {100, 10},
                {100, 100},
                {200, 100},
                {500, 100},
                {null, null},
                {null, 1},
                {1, null},
                {null, -1},
                {-1, null}
        };

        for (Object[] arg : args) {
            Object arg0 = arg[0];
            Object arg1 = arg[1];
            try {

                String windowLengthCount = null;
                if (arg0 != null) {
                    windowLengthCount = String.valueOf(arg0);
                }
                String slidingIntervalCount = null;

                if (arg1 != null) {
                    slidingIntervalCount = String.valueOf(arg1);
                }
                context = Mockito.mock(Context.class);
                Mockito.doReturn("test-function").when(context).getFunctionName();
                Mockito.doReturn("test-namespace").when(context).getNamespace();
                Mockito.doReturn("test-tenant").when(context).getTenant();
                Mockito.doReturn(TestTimestampExtractor.class.getName()).when(context).getUserConfigValue
                        ("timestampExtractorClassName");
                Mockito.doReturn("test-source-topic").when(context).getTopicName();
                Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
                Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
                Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
                Mockito.doReturn(windowLengthCount).when(context).getUserConfigValue("windowLengthCount");
                Mockito.doReturn(slidingIntervalCount).when(context).getUserConfigValue("slidingIntervalCount");

                testWindowedPulsarFunction = new TestWindowedPulsarFunction();
                testWindowedPulsarFunction.process(10L, context);

                if (arg0 == null) {
                    fail(String.format("Window length cannot be null -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if ((Integer) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if (arg1 != null && (Integer) arg1 <= 0) {
                    fail(String.format("Sliding interval length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }

                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWindowLengthCount().intValue(),
                        Integer.parseInt(windowLengthCount));
                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getSlidingIntervalCount().intValue(),
                        Integer.parseInt(slidingIntervalCount));
            } catch (IllegalArgumentException e) {
                if (arg0 != null && arg1 != null && (Integer) arg0 > 0 && (Integer) arg1 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthCount: %s "
                            + "slidingIntervalCount: %s", e.getMessage(), arg0, arg1));
                }
            }
        }
    }

    @Test
    public void testSettingSlidingTimeWindow() throws Exception {
        final Object[][] args = new Object[][]{
                {-1L, 10L},
                {10L, -1L},
                {0L, 10L},
                {10L, 0L},
                {0L, 0L},
                {-1L, -1L},
                {5L, 10L},
                {1L, 1L},
                {10L, 5L},
                {100L, 10L},
                {100L, 100L},
                {200L, 100L},
                {500L, 100L},
                {null, null},
                {null, 1L},
                {1L, null},
                {null, -1L},
                {-1L, null}
        };

        for (Object[] arg : args) {
            Object arg0 = arg[0];
            Object arg1 = arg[1];
            try {
                String windowLengthDuration = null;
                if (arg0 != null) {
                    windowLengthDuration = String.valueOf(arg0);
                }
                String slidingIntervalDuration = null;

                if (arg1 != null) {
                    slidingIntervalDuration = String.valueOf(arg1);
                }
                context = Mockito.mock(Context.class);
                Mockito.doReturn("test-function").when(context).getFunctionName();
                Mockito.doReturn("test-namespace").when(context).getNamespace();
                Mockito.doReturn("test-tenant").when(context).getTenant();
                Mockito.doReturn(TestTimestampExtractor.class.getName()).when(context).getUserConfigValue
                        ("timestampExtractorClassName");
                Mockito.doReturn("test-source-topic").when(context).getTopicName();
                Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
                Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
                Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
                Mockito.doReturn(windowLengthDuration).when(context).getUserConfigValue("windowLengthDurationMs");
                Mockito.doReturn(slidingIntervalDuration)
                        .when(context).getUserConfigValue("slidingIntervalDurationMs");

                testWindowedPulsarFunction = new TestWindowedPulsarFunction();
                testWindowedPulsarFunction.process(10L, context);

                if (arg0 == null) {
                    fail(String.format("Window length cannot be null -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if ((Long) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if (arg1 != null && (Long) arg1 <= 0) {
                    fail(String.format("Sliding interval length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }

                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWindowLengthDurationMs().longValue(),
                        Long.parseLong(windowLengthDuration));
                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getSlidingIntervalDurationMs().longValue(),
                        Long.parseLong(slidingIntervalDuration));
            } catch (IllegalArgumentException e) {
                if (arg0 != null && arg1 != null && (Long) arg0 > 0 && (Long) arg1 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthDuration: %s "
                            + "slidingIntervalDuration: %s", e.getMessage(), arg0, arg1));
                }
            }
        }
    }

    @Test
    public void testSettingTumblingCountWindow() throws Exception {
        final Object[] args = new Object[]{-1, 0, 1, 2, 5, 10, null};

        for (Object arg : args) {
            Object arg0 = arg;
            try {

                String windowLengthCount = null;
                if (arg0 != null) {
                    windowLengthCount = String.valueOf(arg0);
                }

                context = Mockito.mock(Context.class);
                Mockito.doReturn("test-function").when(context).getFunctionName();
                Mockito.doReturn("test-namespace").when(context).getNamespace();
                Mockito.doReturn("test-tenant").when(context).getTenant();
                Mockito.doReturn(TestTimestampExtractor.class.getName()).when(context).getUserConfigValue
                        ("timestampExtractorClassName");
                Mockito.doReturn("test-source-topic").when(context).getTopicName();
                Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
                Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
                Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
                Mockito.doReturn(windowLengthCount).when(context).getUserConfigValue("windowLengthCount");

                testWindowedPulsarFunction = new TestWindowedPulsarFunction();
                testWindowedPulsarFunction.process(10L, context);

                if (arg0 == null) {
                    fail(String.format("Window length cannot be null -- windowLengthCount: %s", arg0));
                }
                if ((Integer) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- windowLengthCount: %s",
                            arg0));
                }

                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWindowLengthCount().intValue(),
                        Integer.parseInt(windowLengthCount));
                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWindowLengthCount(),
                        testWindowedPulsarFunction.windowConfig.getSlidingIntervalCount());
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Integer) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthCount: %s", e
                            .getMessage(), arg0));
                }
            }
        }
    }

    @Test
    public void testSettingTumblingTimeWindow() throws Exception {
        final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
        for (Object arg : args) {
            Object arg0 = arg;
            try {

                String windowLengthDuration = null;
                if (arg0 != null) {
                    windowLengthDuration = String.valueOf(arg0);
                }

                context = Mockito.mock(Context.class);
                Mockito.doReturn("test-function").when(context).getFunctionName();
                Mockito.doReturn("test-namespace").when(context).getNamespace();
                Mockito.doReturn("test-tenant").when(context).getTenant();
                Mockito.doReturn(TestTimestampExtractor.class.getName()).when(context).getUserConfigValue
                        ("timestampExtractorClassName");
                Mockito.doReturn("test-source-topic").when(context).getTopicName();
                Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
                Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
                Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
                Mockito.doReturn(windowLengthDuration).when(context).getUserConfigValue("windowLengthDurationMs");

                testWindowedPulsarFunction = new TestWindowedPulsarFunction();
                testWindowedPulsarFunction.process(10L, context);

                if (arg0 == null) {
                    fail(String.format("Window count duration cannot be null -- windowLengthDuration: %s",
                            arg0));
                }
                if ((Long) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- windowLengthDuration: %s",
                            arg0));
                }
                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWindowLengthDurationMs().longValue(),
                        Long.parseLong(windowLengthDuration));
                Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWindowLengthDurationMs(),
                        testWindowedPulsarFunction.windowConfig.getSlidingIntervalDurationMs());
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Long) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthDuration: %s", e
                            .getMessage(), arg0));
                }
            }
        }
    }

    @Test
    public void testSettingLagTime() throws Exception {
        final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
        for (Object arg : args) {
            Object arg0 = arg;
            try {

                String maxLagMs = null;
                if (arg0 != null) {
                    maxLagMs = String.valueOf(arg0);
                }

                context = Mockito.mock(Context.class);
                Mockito.doReturn("test-function").when(context).getFunctionName();
                Mockito.doReturn("test-namespace").when(context).getNamespace();
                Mockito.doReturn("test-tenant").when(context).getTenant();
                Mockito.doReturn(TestTimestampExtractor.class.getName()).when(context).getUserConfigValue
                        ("timestampExtractorClassName");
                Mockito.doReturn("test-source-topic").when(context).getTopicName();
                Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
                Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
                Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
                Mockito.doReturn("1").when(context).getUserConfigValue("windowLengthCount");
                Mockito.doReturn("1").when(context).getUserConfigValue("slidingIntervalCount");
                Mockito.doReturn(maxLagMs).when(context).getUserConfigValue("maxLagMs");

                testWindowedPulsarFunction = new TestWindowedPulsarFunction();
                testWindowedPulsarFunction.process(10L, context);

                if (arg0 == null) {
                    Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getMaxLagMs(),
                            new Long(testWindowedPulsarFunction.DEFAULT_MAX_LAG_MS));
                } else if((Long) arg0 <= 0) {
                    fail(String.format("Window lag cannot be zero or less -- lagTime: %s", arg0));
                } else {
                    Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getMaxLagMs().longValue(),
                            Long.parseLong(maxLagMs));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Long) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- lagTime: %s",
                            e.getMessage(), arg0));
                }
            }
        }
    }

    @Test
    public void testSettingWaterMarkInterval() throws Exception {
        final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
        for (Object arg : args) {
            Object arg0 = arg;
            try {

                String watermarkEmitInterval = null;
                if (arg0 != null) {
                    watermarkEmitInterval = String.valueOf(arg0);
                }

                context = Mockito.mock(Context.class);
                Mockito.doReturn("test-function").when(context).getFunctionName();
                Mockito.doReturn("test-namespace").when(context).getNamespace();
                Mockito.doReturn("test-tenant").when(context).getTenant();
                Mockito.doReturn(TestTimestampExtractor.class.getName()).when(context).getUserConfigValue
                        ("timestampExtractorClassName");
                Mockito.doReturn("test-source-topic").when(context).getTopicName();
                Mockito.doReturn(Utf8StringSerDe.class.getName()).when(context).getOutputSerdeClassName();
                Mockito.doReturn(Collections.singleton("test-source-topic")).when(context).getSourceTopics();
                Mockito.doReturn("test-sink-topic").when(context).getSinkTopic();
                Mockito.doReturn("1").when(context).getUserConfigValue("windowLengthCount");
                Mockito.doReturn("1").when(context).getUserConfigValue("slidingIntervalCount");
                Mockito.doReturn(watermarkEmitInterval).when(context).getUserConfigValue("watermarkEmitIntervalMs");

                testWindowedPulsarFunction = new TestWindowedPulsarFunction();
                testWindowedPulsarFunction.process(10L, context);

                if (arg0 == null) {
                    Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWatermarkEmitIntervalMs(),
                            new Long(testWindowedPulsarFunction.DEFAULT_WATERMARK_EVENT_INTERVAL_MS));
                } else if ((Long) arg0 <= 0) {
                    fail(String.format("Watermark interval cannot be zero or less -- watermarkInterval: "
                            + "%s", arg0));
                } else {
                    Assert.assertEquals(testWindowedPulsarFunction.windowConfig.getWatermarkEmitIntervalMs().longValue(),
                            Long.parseLong(watermarkEmitInterval));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Long) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- watermarkInterval: %s", e
                            .getMessage(), arg0));
                }
            }
        }
    }
}

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
package org.apache.pulsar.functions.windowing;

import com.google.gson.Gson;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;

import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.functions.api.WindowContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

/**
 * Unit tests for {@link WindowFunctionExecutor}
 */
public class WindowFunctionExecutorTest {

    private static class TestWindowFunctionExecutor extends WindowFunctionExecutor<Long, Long> {

        List<Window<Record<Long>>> windows = new ArrayList<>();

        @Override
        public Long process(Window<Record<Long>> inputWindow, WindowContext context) throws Exception {
            windows.add(inputWindow);
            return null;
        }
    }

    private static class TestFunction implements Function<Collection<Long>, Long> {

        @Override
        public Long apply(Collection<Long> longs) {
            return null;
        }
    }

    private static class TestWrongFunction implements Function<Long, Long> {

        @Override
        public Long apply(Long aLong) {
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
            return Long.parseLong(input);
        }
    }


    private TestWindowFunctionExecutor testWindowedPulsarFunction;
    private Context context;
    private WindowConfig windowConfig;

    @BeforeMethod
    public void setUp() {
        testWindowedPulsarFunction = new TestWindowFunctionExecutor();
        context = mock(Context.class);
        doReturn("test-function").when(context).getFunctionName();
        doReturn("test-namespace").when(context).getNamespace();
        doReturn("test-tenant").when(context).getTenant();

        Record<?> record = mock(Record.class);
        doReturn(Optional.of("test-topic")).when(record).getTopicName();
        doReturn(record).when(context).getCurrentRecord();

        windowConfig = new WindowConfig();
        windowConfig.setTimestampExtractorClassName(TestTimestampExtractor.class.getName());
        windowConfig.setWindowLengthDurationMs(20L);
        windowConfig.setSlidingIntervalDurationMs(10L);
        windowConfig.setMaxLagMs(5L);
        // trigger manually to avoid timing issues
        windowConfig.setWatermarkEmitIntervalMs(100000L);
        windowConfig.setActualWindowFunctionClassName(TestFunction.class.getName());
        doReturn(Optional.of(new Gson().fromJson(new Gson().toJson(windowConfig), Map.class))).when(context).getUserConfigValue(WindowConfig.WINDOW_CONFIG_KEY);

        doReturn(Collections.singleton("test-source-topic")).when(context).getInputTopics();
        doReturn("test-sink-topic").when(context).getOutputTopic();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanUp() {
        testWindowedPulsarFunction.shutdown();
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testExecuteWithWrongWrongTimestampExtractorType() throws Exception {
        WindowConfig windowConfig = new WindowConfig();
        windowConfig.setTimestampExtractorClassName(TestWrongTimestampExtractor.class.getName());
        doReturn(Optional.of(new Gson().fromJson(new Gson().toJson(windowConfig), Map.class)))
                .when(context).getUserConfigValue(WindowConfig.WINDOW_CONFIG_KEY);

        testWindowedPulsarFunction.process(10L, context);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExecuteWithWrongJavaWindowFunctionType() throws Exception {
        WindowConfig windowConfig = new WindowConfig();
        windowConfig.setActualWindowFunctionClassName(TestWrongFunction.class.getName());
        doReturn(Optional.of(new Gson().fromJson(new Gson().toJson(windowConfig), Map.class)))
                .when(context).getUserConfigValue(WindowConfig.WINDOW_CONFIG_KEY);

        testWindowedPulsarFunction.process(10L, context);
    }

    @Test
    public void testExecuteWithTs() throws Exception {
        long[] timestamps = {603, 605, 607, 618, 626, 636};
        for (long ts : timestamps) {
            Record<?> record = mock(Record.class);
            doReturn(Optional.of("test-topic")).when(record).getTopicName();
            doReturn(record).when(context).getCurrentRecord();
            doReturn(ts).when(record).getValue();
            testWindowedPulsarFunction.process(ts, context);
        }
        testWindowedPulsarFunction.waterMarkEventGenerator.run();
        assertEquals(3, testWindowedPulsarFunction.windows.size());
        Window<Record<Long>> first = testWindowedPulsarFunction.windows.get(0);
        assertArrayEquals(
                new long[]{603, 605, 607},
                new long[]{first.get().get(0).getValue(), first.get().get(1).getValue(), first.get().get(2).getValue()});

        Window<Record<Long>> second = testWindowedPulsarFunction.windows.get(1);
        assertArrayEquals(
                new long[]{603, 605, 607, 618},
                new long[]{second.get().get(0).getValue(), second.get().get(1).getValue(), second.get().get(2).getValue(), second.get().get(3).getValue()});

        Window<Record<Long>> third = testWindowedPulsarFunction.windows.get(2);
        assertArrayEquals(new long[]{618, 626}, new long[]{third.get().get(0).getValue(), third.get().get(1).getValue()});
    }

    @Test
    public void testPrepareLateTupleStreamWithoutTs() throws Exception {
        context = mock(Context.class);
        doReturn("test-function").when(context).getFunctionName();
        doReturn("test-namespace").when(context).getNamespace();
        doReturn("test-tenant").when(context).getTenant();
        doReturn(Collections.singleton("test-source-topic")).when(context).getInputTopics();
        doReturn("test-sink-topic").when(context).getOutputTopic();
        WindowConfig windowConfig = new WindowConfig();
        windowConfig.setWindowLengthDurationMs(20L);
        windowConfig.setSlidingIntervalDurationMs(10L);
        windowConfig.setLateDataTopic("$late");
        windowConfig.setMaxLagMs(5L);
        windowConfig.setWatermarkEmitIntervalMs(10L);
        windowConfig.setActualWindowFunctionClassName(TestFunction.class.getName());
        doReturn(Optional.of(new Gson().fromJson(new Gson().toJson(windowConfig), Map.class)))
                .when(context).getUserConfigValue(WindowConfig.WINDOW_CONFIG_KEY);

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

        windowConfig.setLateDataTopic("$late");
        doReturn(Optional.of(new Gson().fromJson(new Gson().toJson(windowConfig), Map.class)))
                .when(context).getUserConfigValue(WindowConfig.WINDOW_CONFIG_KEY);
        TypedMessageBuilder typedMessageBuilder = mock(TypedMessageBuilder.class);
        when(typedMessageBuilder.value(any())).thenReturn(typedMessageBuilder);
        when(typedMessageBuilder.sendAsync()).thenReturn(CompletableFuture.anyOf());
        when(context.newOutputMessage(anyString(), any())).thenReturn(typedMessageBuilder);

        long[] timestamps = {603, 605, 607, 618, 626, 636, 600};
        List<Long> events = new ArrayList<>(timestamps.length);

        for (long ts : timestamps) {
            events.add(ts);
            Record<?> record = mock(Record.class);
            doReturn(Optional.of("test-topic")).when(record).getTopicName();
            doReturn(record).when(context).getCurrentRecord();
            doReturn(ts).when(record).getValue();
            testWindowedPulsarFunction.process(ts, context);

            //Update the watermark to this timestamp
            testWindowedPulsarFunction.waterMarkEventGenerator.run();
        }
        System.out.println(testWindowedPulsarFunction.windows);
        long event = events.get(events.size() - 1);
    }
}

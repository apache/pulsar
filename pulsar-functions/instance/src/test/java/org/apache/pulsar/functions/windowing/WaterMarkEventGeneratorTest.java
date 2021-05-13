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

import org.apache.pulsar.functions.api.Context;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link WaterMarkEventGenerator}
 */
public class WaterMarkEventGeneratorTest {
    private WaterMarkEventGenerator<Integer> waterMarkEventGenerator;
    private WindowManager<Integer> windowManager;
    private List<Event<Integer>> eventList = new ArrayList<>();
    private Context context;

    @BeforeMethod
    public void setUp() {
        windowManager = new WindowManager<Integer>(null, new LinkedList<>()) {
            @Override
            public void add(Event<Integer> event) {
                eventList.add(event);
            }
        };

        context = Mockito.mock(Context.class);
        Mockito.doReturn("test-function").when(context).getFunctionName();
        Mockito.doReturn("test-namespace").when(context).getNamespace();
        Mockito.doReturn("test-tenant").when(context).getTenant();
        // set watermark interval to a high value and trigger manually to fix timing issues
        waterMarkEventGenerator = new WaterMarkEventGenerator<>(windowManager, 5L, 5, Collections
                .singleton("s1"), context);
//        waterMarkEventGenerator.start();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
//        waterMarkEventGenerator.shutdown();
        eventList.clear();
    }

    @Test
    public void testTrackSingleStream() throws Exception {
        waterMarkEventGenerator.track("s1", 100);
        waterMarkEventGenerator.track("s1", 110);
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(105, eventList.get(0).getTimestamp());
    }

    @Test
    public void testTrackSingleStreamOutOfOrder() throws Exception {
        waterMarkEventGenerator.track("s1", 100);
        waterMarkEventGenerator.track("s1", 110);
        waterMarkEventGenerator.track("s1", 104);
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(105, eventList.get(0).getTimestamp());
    }

    @Test
    public void testTrackTwoStreams() throws Exception {
        Set<String> streams = new HashSet<>();
        streams.add("s1");
        streams.add("s2");
        waterMarkEventGenerator = new WaterMarkEventGenerator<>(windowManager, 5L,
                5, streams, context);
        waterMarkEventGenerator.start();

        waterMarkEventGenerator.track("s1", 100);
        waterMarkEventGenerator.track("s1", 110);
        waterMarkEventGenerator.run();
        assertTrue(eventList.isEmpty());
        waterMarkEventGenerator.track("s2", 95);
        waterMarkEventGenerator.track("s2", 98);
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(93, eventList.get(0).getTimestamp());
    }

    @Test
    public void testNoEvents() throws Exception {
        waterMarkEventGenerator.run();
        assertTrue(eventList.isEmpty());
    }

    @Test
    public void testLateEvent() throws Exception {
        assertTrue(waterMarkEventGenerator.track("s1", 100));
        assertTrue(waterMarkEventGenerator.track("s1", 110));
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(105, eventList.get(0).getTimestamp());
        eventList.clear();
        assertTrue(waterMarkEventGenerator.track("s1", 105));
        assertTrue(waterMarkEventGenerator.track("s1", 106));
        assertTrue(waterMarkEventGenerator.track("s1", 115));
        assertFalse(waterMarkEventGenerator.track("s1", 104));
        waterMarkEventGenerator.run();
        assertTrue(eventList.get(0).isWatermark());
        assertEquals(110, eventList.get(0).getTimestamp());
    }
}

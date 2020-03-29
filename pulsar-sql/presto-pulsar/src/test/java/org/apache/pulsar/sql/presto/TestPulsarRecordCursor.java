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
package org.apache.pulsar.sql.presto;

import io.airlift.log.Logger;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPulsarRecordCursor extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarRecordCursor.class);

    @Test
    public void testTopics() throws Exception {

        for (Map.Entry<TopicName, PulsarRecordCursor> entry : pulsarRecordCursors.entrySet()) {

            log.info("!------ topic %s ------!", entry.getKey());
            setup();
            PulsarRecordCursor pulsarRecordCursor = entry.getValue();
            TopicName topicName = entry.getKey();

            int count = 0;
            while (pulsarRecordCursor.advanceNextPosition()) {
                List<String> columnsSeen = new LinkedList<>();
                for (int i = 0; i < fooColumnHandles.size(); i++) {
                    if (pulsarRecordCursor.isNull(i)) {
                        columnsSeen.add(fooColumnHandles.get(i).getName());
                    } else {
                        if (fooColumnHandles.get(i).getName().equals("field1")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Integer) fooFunctions.get("field1").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field2")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), ((String) fooFunctions.get("field2").apply(count)).getBytes());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field3")) {
                            assertEquals(pulsarRecordCursor.getLong(i), Float.floatToIntBits(((Float) fooFunctions.get("field3").apply(count)).floatValue()));
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field4")) {
                            assertEquals(pulsarRecordCursor.getDouble(i), ((Double) fooFunctions.get("field4").apply(count)).doubleValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field5")) {
                            assertEquals(pulsarRecordCursor.getBoolean(i), ((Boolean) fooFunctions.get("field5").apply(count)).booleanValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field6")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Long) fooFunctions.get("field6").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("timestamp")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("time")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("date")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.field1")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Integer) fooFunctions.get("bar.field1").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.field2")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), ((String) fooFunctions.get("bar.field2").apply(count)).getBytes());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.field3")) {
                            assertEquals(pulsarRecordCursor.getLong(i), Float.floatToIntBits(((Float) fooFunctions.get("bar.field3").apply(count)).floatValue()));
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test.field4")) {
                            assertEquals(pulsarRecordCursor.getDouble(i), ((Double) fooFunctions.get("bar.test.field4").apply(count)).doubleValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test.field5")) {
                            assertEquals(pulsarRecordCursor.getBoolean(i), ((Boolean) fooFunctions.get("bar.test.field5").apply(count)).booleanValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test.field6")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Long) fooFunctions.get("bar.test.field6").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test.foobar.field1")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Integer) fooFunctions.get("bar.test.foobar.field1").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test2.field4")) {
                            assertEquals(pulsarRecordCursor.getDouble(i), ((Double) fooFunctions.get("bar.test2.field4").apply(count)).doubleValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test2.field5")) {
                            assertEquals(pulsarRecordCursor.getBoolean(i), ((Boolean) fooFunctions.get("bar.test2.field5").apply(count)).booleanValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test2.field6")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Long) fooFunctions.get("bar.test2.field6").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar.test2.foobar.field1")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Integer) fooFunctions.get("bar.test2.foobar.field1").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field7")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), fooFunctions.get("field7").apply(count).toString().getBytes());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else {
                            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(fooColumnHandles.get(i).getName())) {
                                columnsSeen.add(fooColumnHandles.get(i).getName());
                            }
                        }
                    }
                }
                assertEquals(columnsSeen.size(), fooColumnHandles.size());
                count++;
            }
            assertEquals(count, topicsToNumEntries.get(topicName.getSchemaName()).longValue());
            assertEquals(pulsarRecordCursor.getCompletedBytes(), completedBytes);
            cleanup();
            pulsarRecordCursor.close();
        }
    }
}

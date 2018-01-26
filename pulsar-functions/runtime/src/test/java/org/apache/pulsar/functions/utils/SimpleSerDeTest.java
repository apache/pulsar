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

package org.apache.pulsar.functions.utils;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * Unit test of {@link SimpleSerDe}.
 */
public class SimpleSerDeTest {
    @Test
    public void testStringSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(String.class, true);
        SimpleSerDe deserializer = new SimpleSerDe(String.class, false);
        String input = new String("input");
        byte[] output = serializer.serialize(input);
        String result = (String) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(String.class, false);
            serDe.serialize(new String("input"));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(String.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testLongSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(Long.class, true);
        SimpleSerDe deserializer = new SimpleSerDe(Long.class, false);
        Long input = new Long(648292);
        byte[] output = serializer.serialize(input);
        Long result = (Long) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(Long.class, false);
            serDe.serialize(new Long(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(Long.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testDoubleSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(Double.class, true);
        SimpleSerDe deserializer = new SimpleSerDe(Double.class, false);
        Double input = new Double(648292.32432);
        byte[] output = serializer.serialize(input);
        Double result = (Double) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(Double.class, false);
            serDe.serialize(new Double(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(Double.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testFloatSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(Float.class, true);
        SimpleSerDe deserializer = new SimpleSerDe(Float.class, false);
        Float input = new Float(354353.54654);
        byte[] output = serializer.serialize(input);
        Float result = (Float) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(Float.class, false);
            serDe.serialize(new Float(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(Float.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testIntegerSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(Integer.class, true);
        SimpleSerDe deserializer = new SimpleSerDe(Integer.class, false);
        Integer input = new Integer(2542352);
        byte[] output = serializer.serialize(input);
        Integer result = (Integer) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(Integer.class, false);
            serDe.serialize(new Integer(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(Integer.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testMapSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(Map.class, true);
        SimpleSerDe deserializer = new SimpleSerDe(Map.class, false);
        Map<String, String> input = new HashMap<>();
        input.put("Key", "Value");
        byte[] output = serializer.serialize(input);
        Map result = (Map) deserializer.deserialize(output);
        assertEquals(result.size(), input.size());
        assertEquals(result.get("Key"), input.get("Key"));

        try {
            SimpleSerDe serDe = new SimpleSerDe(Map.class, false);
            serDe.serialize(new HashMap());
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(Map.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testListSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(List.class, true);
        SimpleSerDe deserializer = new SimpleSerDe(List.class, false);
        List<Integer> input = new LinkedList<>();
        input.add(1234);
        byte[] output = serializer.serialize(input);
        List result = (List) deserializer.deserialize(output);
        assertEquals(result.size(), input.size());
        assertEquals(result.get(0), input.get(0));

        try {
            SimpleSerDe serDe = new SimpleSerDe(List.class, false);
            serDe.serialize(new LinkedList<>());
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(List.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }
}

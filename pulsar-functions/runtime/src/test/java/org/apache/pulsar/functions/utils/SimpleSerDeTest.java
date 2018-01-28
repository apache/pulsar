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

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.SerDe;
import org.testng.annotations.Test;


import static org.testng.Assert.*;

/**
 * Unit test of {@link SimpleSerDe}.
 */
public class SimpleSerDeTest {
    @Test
    public void testStringSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(new String(), true);
        SimpleSerDe deserializer = new SimpleSerDe(new String(), false);
        String input = new String("input");
        byte[] output = serializer.serialize(input);
        String result = (String) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(new String(), false);
            serDe.serialize(new String("input"));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(new String(), true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testLongSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(new Long(0), true);
        SimpleSerDe deserializer = new SimpleSerDe(new Long(0), false);
        Long input = new Long(648292);
        byte[] output = serializer.serialize(input);
        Long result = (Long) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Long(0), false);
            serDe.serialize(new Long(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Long(0), true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testDoubleSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(new Double(0), true);
        SimpleSerDe deserializer = new SimpleSerDe(new Double(0), false);
        Double input = new Double(648292.32432);
        byte[] output = serializer.serialize(input);
        Double result = (Double) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Double(0), false);
            serDe.serialize(new Double(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Double(0), true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testFloatSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(new Float(0), true);
        SimpleSerDe deserializer = new SimpleSerDe(new Float(0), false);
        Float input = new Float(354353.54654);
        byte[] output = serializer.serialize(input);
        Float result = (Float) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Float(0), false);
            serDe.serialize(new Float(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Float(0), true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testIntegerSerDe() {
        SimpleSerDe serializer = new SimpleSerDe(new Integer(0), true);
        SimpleSerDe deserializer = new SimpleSerDe(new Integer(0), false);
        Integer input = new Integer(2542352);
        byte[] output = serializer.serialize(input);
        Integer result = (Integer) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Integer(0), false);
            serDe.serialize(new Integer(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            SimpleSerDe serDe = new SimpleSerDe(new Integer(0), true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    private class SimplePulsarFunction implements PulsarFunction<String, String> {
        @Override
        public String process(String input, Context context) {
            return null;
        }
    }

    @Test
    public void testPulsarFunction() {
        SimplePulsarFunction pulsarFunction = new SimplePulsarFunction();
        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
        SerDe serDe = new SimpleSerDe(new String(), false);
        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
        assertTrue(inputSerdeTypeArgs[0].isAssignableFrom(typeArgs[0]));
    }

}

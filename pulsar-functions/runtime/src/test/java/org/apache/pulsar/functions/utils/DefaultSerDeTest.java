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
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.testng.annotations.Test;


import static org.testng.Assert.*;

/**
 * Unit test of {@link DefaultSerDe}.
 */
public class DefaultSerDeTest {
    @Test
    public void testStringSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(String.class, true);
        DefaultSerDe deserializer = new DefaultSerDe(String.class, false);
        String input = new String("input");
        byte[] output = serializer.serialize(input);
        String result = (String) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            DefaultSerDe serDe = new DefaultSerDe(String.class, false);
            serDe.serialize(new String("input"));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            DefaultSerDe serDe = new DefaultSerDe(String.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testLongSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Long.class, true);
        DefaultSerDe deserializer = new DefaultSerDe(Long.class, false);
        Long input = new Long(648292);
        byte[] output = serializer.serialize(input);
        Long result = (Long) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            DefaultSerDe serDe = new DefaultSerDe(Long.class, false);
            serDe.serialize(new Long(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            DefaultSerDe serDe = new DefaultSerDe(Long.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testDoubleSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Double.class, true);
        DefaultSerDe deserializer = new DefaultSerDe(Double.class, false);
        Double input = new Double(648292.32432);
        byte[] output = serializer.serialize(input);
        Double result = (Double) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            DefaultSerDe serDe = new DefaultSerDe(Double.class, false);
            serDe.serialize(new Double(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            DefaultSerDe serDe = new DefaultSerDe(Double.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testFloatSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Float.class, true);
        DefaultSerDe deserializer = new DefaultSerDe(Float.class, false);
        Float input = new Float(354353.54654);
        byte[] output = serializer.serialize(input);
        Float result = (Float) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            DefaultSerDe serDe = new DefaultSerDe(Float.class, false);
            serDe.serialize(new Float(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            DefaultSerDe serDe = new DefaultSerDe(Float.class, true);
            serDe.deserialize(new byte[10]);
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }
    }

    @Test
    public void testIntegerSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Integer.class, true);
        DefaultSerDe deserializer = new DefaultSerDe(Integer.class, false);
        Integer input = new Integer(2542352);
        byte[] output = serializer.serialize(input);
        Integer result = (Integer) deserializer.deserialize(output);
        assertEquals(result, input);

        try {
            DefaultSerDe serDe = new DefaultSerDe(Integer.class, false);
            serDe.serialize(new Integer(34242));
            assertFalse(true);
        } catch (Exception ex) {
            // This is good
        }

        try {
            DefaultSerDe serDe = new DefaultSerDe(Integer.class, true);
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
        SerDe serDe = new DefaultSerDe(String.class, false);
        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
        assertTrue(inputSerdeTypeArgs[0].isAssignableFrom(typeArgs[0]));
    }

}

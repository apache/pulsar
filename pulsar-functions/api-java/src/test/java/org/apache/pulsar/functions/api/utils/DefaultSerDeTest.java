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

package org.apache.pulsar.functions.api.utils;

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.SerDe;
import org.testng.annotations.Test;


import static org.testng.Assert.*;

/**
 * Unit test of {@link DefaultSerDe}.
 */
public class DefaultSerDeTest {
    @Test
    public void testStringSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(String.class);
        DefaultSerDe deserializer = new DefaultSerDe(String.class);
        String input = new String("input");
        byte[] output = serializer.serialize(input);
        String result = (String) deserializer.deserialize(output);
        assertEquals(result, input);
    }

    @Test
    public void testLongSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Long.class);
        DefaultSerDe deserializer = new DefaultSerDe(Long.class);
        Long input = new Long(648292);
        byte[] output = serializer.serialize(input);
        Long result = (Long) deserializer.deserialize(output);
        assertEquals(result, input);
    }

    @Test
    public void testDoubleSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Double.class);
        DefaultSerDe deserializer = new DefaultSerDe(Double.class);
        Double input = new Double(648292.32432);
        byte[] output = serializer.serialize(input);
        Double result = (Double) deserializer.deserialize(output);
        assertEquals(result, input);
    }

    @Test
    public void testFloatSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Float.class);
        DefaultSerDe deserializer = new DefaultSerDe(Float.class);
        Float input = new Float(354353.54654);
        byte[] output = serializer.serialize(input);
        Float result = (Float) deserializer.deserialize(output);
        assertEquals(result, input);
    }

    @Test
    public void testIntegerSerDe() {
        DefaultSerDe serializer = new DefaultSerDe(Integer.class);
        DefaultSerDe deserializer = new DefaultSerDe(Integer.class);
        Integer input = new Integer(2542352);
        byte[] output = serializer.serialize(input);
        Integer result = (Integer) deserializer.deserialize(output);
        assertEquals(result, input);
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
        SerDe serDe = new DefaultSerDe(String.class);
        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
        assertTrue(inputSerdeTypeArgs[0].isAssignableFrom(typeArgs[0]));
    }

}

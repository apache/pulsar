/*
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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class FieldParserTest {

    @Test
    public void testMap() {
        Map<String, String> properties = new HashMap<>();
        properties.put("name", "config");
        properties.put("stringStringMap", "key1=value1,key2=value2");
        properties.put("stringIntMap", "key1=1,key2=2");
        properties.put("longStringMap", "1=value1,2=value2");

        MyConfig config = new MyConfig();
        FieldParser.update(properties, config);
        assertEquals(config.name, "config");
        assertEquals(config.stringStringMap.get("key1"), "value1");
        assertEquals(config.stringStringMap.get("key2"), "value2");

        assertEquals((int) config.stringIntMap.get("key1"), 1);
        assertEquals((int) config.stringIntMap.get("key2"), 2);

        assertEquals(config.longStringMap.get(1L), "value1");
        assertEquals(config.longStringMap.get(2L), "value2");

    }

    @Test
    public void testWithBlankVallueConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("name", "  config   ");
        properties.put("stringStringMap", "key1=value1  , key2=   value2  ");
        properties.put("stringIntMap", "key1 = 1, key2 =  2 ");
        properties.put("longStringMap", " 1 =value1  ,2  =value2    ");
        properties.put("longList", " 1, 3,  8 , 0  ,9   ");
        properties.put("stringList", "  aa, bb   ,  cc, ee  ");
        properties.put("longSet", " 1, 3,  8 , 0 , 3, 1   ,9   ");
        properties.put("stringSet", "  aa, bb   ,  cc, ee , bb,  aa ");

        MyConfig config = new MyConfig();
        FieldParser.update(properties, config);
        assertEquals(config.name, "config");
        assertEquals(config.stringStringMap.get("key1"), "value1");
        assertEquals(config.stringStringMap.get("key2"), "value2");

        assertEquals((int) config.stringIntMap.get("key1"), 1);
        assertEquals((int) config.stringIntMap.get("key2"), 2);

        assertEquals(config.longStringMap.get(1L), "value1");
        assertEquals(config.longStringMap.get(2L), "value2");

        assertEquals((long)config.longList.get(2), 8);
        assertEquals(config.stringList.get(1), "bb");

        assertTrue(config.longSet.contains(3L));
        assertTrue(config.stringSet.contains("bb"));
    }

    public static class MyConfig {
        public String name;
        public Map<String, String> stringStringMap;
        public Map<String, Integer> stringIntMap;
        public Map<Long, String> longStringMap;
        public List<Long> longList;
        public List<String> stringList;
        public Set<Long> longSet;
        public Set<String> stringSet;
    }

    @Test
    public void testNullStrValue() throws Exception {
        class TestMap {
            public List<String> list;
            public Set<String> set;
            public Map<String, String> map;
            public Optional<String> optional;
        }

        Field listField = TestMap.class.getField("list");
        Object listValue = FieldParser.value(null, listField);
        assertNull(listValue);

        listValue = FieldParser.value("null", listField);
        assertTrue(listValue instanceof List);
        assertEquals(((List) listValue).size(), 1);
        assertEquals(((List) listValue).get(0), "null");


        Field setField = TestMap.class.getField("set");
        Object setValue = FieldParser.value(null, setField);
        assertNull(setValue);

        setValue = FieldParser.value("null", setField);
        assertTrue(setValue instanceof Set);
        assertEquals(((Set) setValue).size(), 1);
        assertEquals(((Set) setValue).iterator().next(), "null");

        Field mapField = TestMap.class.getField("map");
        Object mapValue = FieldParser.value(null, mapField);
        assertNull(mapValue);

        try {
            FieldParser.value("null", mapField);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("null map-value is not in correct format key1=value,key2=value2"));
        }

        Field optionalField = TestMap.class.getField("optional");
        Object optionalValue = FieldParser.value(null, optionalField);
        assertEquals(optionalValue, Optional.empty());
    }
}

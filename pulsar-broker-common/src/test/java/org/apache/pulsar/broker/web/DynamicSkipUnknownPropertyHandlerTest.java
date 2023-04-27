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
package org.apache.pulsar.broker.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class DynamicSkipUnknownPropertyHandlerTest {

    @Test
    public void testHandleUnknownProperty() throws Exception{
        DynamicSkipUnknownPropertyHandler handler = new DynamicSkipUnknownPropertyHandler();
        handler.setSkipUnknownProperty(true);
        // Case 1: initial ObjectMapper with "enable feature".
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.addHandler(handler);
        ObjectReader objectReader = objectMapper.readerFor(TestBean.class);
        // Assert skip unknown property and logging: objectMapper.
        String json = "{\"name1\": \"James\",\"nm\":\"Paul\",\"name2\":\"Eric\"}";
        TestBean testBean = objectMapper.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
        // Assert skip unknown property and logging: objectReader.
        testBean = objectReader.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
        // Assert failure on unknown property.
        handler.setSkipUnknownProperty(false);
        try {
            objectMapper.readValue(json, TestBean.class);
            Assert.fail("Expect UnrecognizedPropertyException when set skipUnknownProperty false.");
        } catch (UnrecognizedPropertyException e){

        }
        try {
            objectReader.readValue(json, TestBean.class);
            Assert.fail("Expect UnrecognizedPropertyException when set skipUnknownProperty false.");
        } catch (UnrecognizedPropertyException e){

        }
        // Case 2: initial ObjectMapper with "disabled feature".
        objectMapper = new ObjectMapper();
        objectMapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.addHandler(handler);
        objectReader = objectMapper.readerFor(TestBean.class);
        // Assert failure on unknown property.
        try {
            objectMapper.readValue(json, TestBean.class);
            Assert.fail("Expect UnrecognizedPropertyException when set skipUnknownProperty false.");
        } catch (UnrecognizedPropertyException e){

        }
        try {
            objectReader.readValue(json, TestBean.class);
            Assert.fail("Expect UnrecognizedPropertyException when set skipUnknownProperty false.");
        } catch (UnrecognizedPropertyException e){

        }
        // Assert skip unknown property and logging.
        handler.setSkipUnknownProperty(true);
        testBean = objectMapper.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
        testBean = objectReader.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
        // Case 3: unknown property deserialize by object json.
        json = "{\"name1\": \"James\",\"nm\":{\"name\":\"Paul\",\"age\":18},\"name2\":\"Eric\"}";
        // Assert skip unknown property and logging.
        handler.setSkipUnknownProperty(true);
        testBean = objectMapper.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
        testBean = objectReader.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
        // Case 4: unknown property deserialize by array json.
        json = "{\"name1\": \"James\",\"nm\":[\"name\",\"Paul\"],\"name2\":\"Eric\"}";
        // Assert skip unknown property and logging.
        handler.setSkipUnknownProperty(true);
        testBean = objectMapper.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
        testBean = objectReader.readValue(json, TestBean.class);
        Assert.assertNull(testBean.getName());
        Assert.assertEquals(testBean.getName1(), "James");
        Assert.assertEquals(testBean.getName2(), "Eric");
    }

    @Data
    private static class TestBean {
        private String name1;
        private String name;
        private String name2;
    }
}
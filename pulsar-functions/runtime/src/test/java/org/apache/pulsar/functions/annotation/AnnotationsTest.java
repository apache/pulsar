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
package org.apache.pulsar.functions.annotation;

import static org.apache.pulsar.functions.annotation.Annotations.verifyAllRequiredFieldsSet;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import lombok.Setter;
import lombok.experimental.Accessors;
import org.testng.annotations.Test;

/**
 * Unit test {@link Annotations}.
 */
public class AnnotationsTest {

    @Accessors(chain = true)
    @Setter
    public static class TestObject {

        @RequiredField
        public String stringField;
        @RequiredField
        public int intField;
        public String optionalField;

    }

    @Test
    public void testVerifyAllRequiredFieldsSet() {
        TestObject to1 = new TestObject();
        assertFalse(verifyAllRequiredFieldsSet(to1));

        TestObject to2 = new TestObject()
            .setStringField("test");
        assertTrue(verifyAllRequiredFieldsSet(to2));

        TestObject to3 = new TestObject()
            .setStringField("test")
            .setIntField(1234);
        assertTrue(verifyAllRequiredFieldsSet(to3));

        TestObject to4 = new TestObject()
            .setStringField("test")
            .setIntField(1234)
            .setOptionalField("optional");
        assertTrue(verifyAllRequiredFieldsSet(to4));
    }

}

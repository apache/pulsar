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
package org.apache.pulsar.config.validation;

import static org.testng.Assert.assertThrows;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ValidatorImplsTest {
    @Test
    public void testPositiveNumberValidator() {
        // test default
        ValidatorImpls.PositiveNumberValidator defaultValidator = new ValidatorImpls.PositiveNumberValidator();
        defaultValidator.validateField("don'tcare", 2);
        assertThrows(IllegalArgumentException.class, () -> defaultValidator.validateField("field", 0));
        assertThrows(IllegalArgumentException.class, () -> defaultValidator.validateField("field", -20));

        // test with valid config
        Map<String, Object> config = new HashMap<>();
        config.put(ConfigValidationAnnotations.ValidatorParams.INCLUDE_ZERO, true);
        ValidatorImpls.PositiveNumberValidator validator = new ValidatorImpls.PositiveNumberValidator(config);
        validator.validateField("don'tcare", 2);
        validator.validateField("field", 0);
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("field", -20));

        config.clear();
        config.put(ConfigValidationAnnotations.ValidatorParams.INCLUDE_ZERO, false);
        ValidatorImpls.PositiveNumberValidator validator1 = new ValidatorImpls.PositiveNumberValidator(config);
        validator1.validateField("don'tcare", 2);
        assertThrows(IllegalArgumentException.class, () -> validator1.validateField("field", 0));
        assertThrows(IllegalArgumentException.class, () -> validator1.validateField("field", -20));
    }

    @Test
    public void testNotNullValidator() {
        ValidatorImpls.NotNullValidator validator = new ValidatorImpls.NotNullValidator();
        validator.validateField("fieldname", 2);
        validator.validateField("fieldname", "Something");
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("field", null));
    }

    @Test
    public void testListEntryTypeValidator() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConfigValidationAnnotations.ValidatorParams.TYPE, String.class);
        ValidatorImpls.ListEntryTypeValidator validator = new ValidatorImpls.ListEntryTypeValidator(config);
        List<String> goodList = new LinkedList<>();
        goodList.add("first");
        goodList.add("second");
        validator.validateField("fieldname", goodList);

        List<Boolean> badList = new LinkedList<>();
        badList.add(true);
        badList.add(false);
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", badList));

        List<Object> mixedList = new LinkedList<>();
        mixedList.add(true);
        mixedList.add("false");
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", mixedList));
   }

    @Test
    public void testMapEntryTypeValidator() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConfigValidationAnnotations.ValidatorParams.KEY_TYPE, String.class);
        config.put(ConfigValidationAnnotations.ValidatorParams.VALUE_TYPE, Integer.class);
        ValidatorImpls.MapEntryTypeValidator validator = new ValidatorImpls.MapEntryTypeValidator(config);
        Map<String, Integer> goodMap = new HashMap<>();
        goodMap.put("first", 1);
        goodMap.put("second", 2);
        validator.validateField("fieldname", goodMap);

        Map<String, Boolean> badKeyMap = new HashMap<>();
        badKeyMap.put("first", true);
        badKeyMap.put("second", false);
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", badKeyMap));

        Map<Integer, Integer> badValueMap = new HashMap<>();
        badValueMap.put(1, 1);
        badValueMap.put(2, 2);
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", badValueMap));
    }

    @Test
    public void testImplementsClassValidator() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConfigValidationAnnotations.ValidatorParams.IMPLEMENTS_CLASS, SocketAddress.class);
        ValidatorImpls.ImplementsClassValidator validator = new ValidatorImpls.ImplementsClassValidator(config);
        validator.validateField("fieldname", SocketAddress.class.getName());
        validator.validateField("fieldname", InetSocketAddress.class.getName());
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", String.class.getName()));
    }

    @Test
    public void testImplementsClassesValidator() {
        Map<String, Object> config = new HashMap<>();
        Class<?> clazzes [] = new Class<?>[] {String.class, Integer.class};
        config.put(ConfigValidationAnnotations.ValidatorParams.IMPLEMENTS_CLASSES, clazzes);
        ValidatorImpls.ImplementsClassesValidator validator = new ValidatorImpls.ImplementsClassesValidator(config);
        validator.validateField("fieldname", String.class.getName());
        validator.validateField("fieldname", Integer.class.getName());
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", Boolean.class.getName()));
    }

    @Test
    public void testStringValidator() {
        Map<String, Object> config = new HashMap<>();
        String accepted [] = new String[] {"good", "bad", "ugly"};
        config.put(ConfigValidationAnnotations.ValidatorParams.ACCEPTED_VALUES, accepted);
        ValidatorImpls.StringValidator validator = new ValidatorImpls.StringValidator(config);
        validator.validateField("fieldname", "good");
        validator.validateField("fieldname", "bad");
        validator.validateField("fieldname", "ugly");
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", "beautiful"));
    }

    @Test
    public void testSimpleTypeValidator() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConfigValidationAnnotations.ValidatorParams.TYPE, Integer.class);
        ValidatorImpls.SimpleTypeValidator validator = new ValidatorImpls.SimpleTypeValidator(config);
        validator.validateField("fieldname", 22);
        assertThrows(IllegalArgumentException.class, () -> validator.validateField("fieldname", "string"));
    }
}

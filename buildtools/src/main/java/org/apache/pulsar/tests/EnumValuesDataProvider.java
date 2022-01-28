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
package org.apache.pulsar.tests;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.DataProvider;

/**
 * TestNG DataProvider for passing all Enum values as parameters to a test method.
 *
 * Supports currently a single Enum parameter for a test method.
 */
public abstract class EnumValuesDataProvider {
    @DataProvider
    public static final Object[][] values(Method testMethod) {
        Class<?> enumClass = Arrays.stream(testMethod.getParameterTypes())
                .findFirst()
                .filter(Class::isEnum)
                .orElseThrow(() -> new IllegalArgumentException("The test method should have an enum parameter."));
        return toDataProviderArray((Class<? extends Enum<?>>) enumClass);
    }

    /*
     * Converts all values of an Enum class to a TestNG DataProvider object array
     */
    public static Object[][] toDataProviderArray(Class<? extends Enum<?>> enumClass) {
        Enum<?>[] enumValues = enumClass.getEnumConstants();
        return Stream.of(enumValues)
                .map(enumValue -> new Object[]{enumValue})
                .collect(Collectors.toList())
                .toArray(new Object[0][]);
    }
}

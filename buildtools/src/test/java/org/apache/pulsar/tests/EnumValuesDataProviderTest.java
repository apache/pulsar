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

import static org.testng.Assert.assertEquals;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class EnumValuesDataProviderTest {
    enum Sample {
        A, B, C
    }

    @Test(dataProviderClass = EnumValuesDataProvider.class, dataProvider = "values")
    void testEnumValuesProvider(Sample sample) {
        System.out.println(sample);
    }

    @Test
    void shouldContainAllEnumValues() {
        verifyTestParameters(EnumValuesDataProvider.toDataProviderArray(Sample.class));
    }

    @Test
    void shouldDetermineEnumValuesFromMethod() {
        Method testMethod = Arrays.stream(getClass().getDeclaredMethods())
                .filter(method -> method.getName().equals("testEnumValuesProvider"))
                .findFirst()
                .get();
        verifyTestParameters(EnumValuesDataProvider.values(testMethod));
    }

    private void verifyTestParameters(Object[][] testParameters) {
        Set<Sample> enumValuesFromDataProvider = Arrays.stream(testParameters)
                .map(element -> element[0])
                .map(Sample.class::cast)
                .collect(Collectors.toSet());
        assertEquals(enumValuesFromDataProvider, new HashSet<>(Arrays.asList(Sample.values())));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    void shouldFailIfEnumParameterIsMissing() {
        Method testMethod = Arrays.stream(getClass().getDeclaredMethods())
                .filter(method -> method.getName().equals("shouldFailIfEnumParameterIsMissing"))
                .findFirst()
                .get();
        EnumValuesDataProvider.values(testMethod);
    }
}
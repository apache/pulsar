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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.TreeSet;
import org.testng.annotations.Test;

public class AppliedTopicPoliciesTest {

    private static Set<String> fieldNames(Class<?> clazz) {
        Set<String> names = new TreeSet<>();
        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isSynthetic() && !java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                names.add(field.getName());
            }
        }
        return names;
    }

    @Test
    public void testEveryTopicPolicyHasAnAppliedCounterpart() {
        // A policy added to TopicPolicies must also be resolved in TopicPolicyResolver and added here.
        Set<String> expected = fieldNames(TopicPolicies.class);
        expected.remove("isGlobal");
        assertEquals(fieldNames(AppliedTopicPolicies.class), expected);
    }
}

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
package org.apache.pulsar.admin.cli;

import com.google.common.collect.Lists;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCmdNamespaceIsolationPolicy {
    @Test
    public void testValidateListMethodToReturnNonNullStringList() {
        List<String> mockList = Lists.newArrayList("", "1", "", "", "1", "2", "3", "4", "", "", "", "1", "");
        List<String> resultList = Lists.newArrayList("1", "1", "2", "3", "4", "1");
        CmdNamespaceIsolationPolicy cmdNamespaceIsolationPolicy = new CmdNamespaceIsolationPolicy(() -> null);
        Class<? extends CmdNamespaceIsolationPolicy> klass = cmdNamespaceIsolationPolicy.getClass();
        Arrays.stream(klass.getDeclaredMethods())
                .filter((innerMethod) -> innerMethod.getName().contains("validateList"))
                .findFirst().ifPresent(innerMethod -> {
            try {
                innerMethod.setAccessible(true);
                List<String> calculatedList = (List<String>) innerMethod.invoke(cmdNamespaceIsolationPolicy, mockList);
                Assert.assertEquals(calculatedList.size(), resultList.size());
                for (int i = 0; i < resultList.size(); i++) {
                    Assert.assertEquals(resultList.get(i), calculatedList.get(i));
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });
    }
}

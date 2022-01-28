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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionInstanceId}.
 */
public class FunctionInstanceIdTest {

    @Test
    public void testThrowsExceptionWhenTenantNamespaceFunctionNameNotProperlyDelimited() {
        try {
            FunctionInstanceId id = new FunctionInstanceId("tenant/namespace:function");

            fail("Expected exception!");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void testThrowsExceptionWhenFunctionInstanceIdNotPropertyDelimited() {
        try {
            FunctionInstanceId id = new FunctionInstanceId("tenant/namespace/function-1");

            fail("Expected exception!");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void testAllowsColonsInFunctionName() {
        FunctionInstanceId id = new FunctionInstanceId("tenant/namespace/my:function:name:-1");

        assertEquals(id.getTenant(), "tenant");
        assertEquals(id.getNamespace(), "namespace");
        assertEquals(id.getName(), "my:function:name");
        assertEquals(id.getInstanceId(), -1);
    }
}

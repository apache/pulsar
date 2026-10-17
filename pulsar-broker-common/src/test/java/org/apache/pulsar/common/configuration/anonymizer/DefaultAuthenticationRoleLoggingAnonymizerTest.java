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
package org.apache.pulsar.common.configuration.anonymizer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import org.testng.annotations.Test;

public class DefaultAuthenticationRoleLoggingAnonymizerTest {

    @Test
    public void testNullRoleIsKeptForEveryType() {
        // originalPrincipal is null for clients that do not connect through a proxy
        for (DefaultRoleAnonymizerType type : DefaultRoleAnonymizerType.values()) {
            assertNull(new DefaultAuthenticationRoleLoggingAnonymizer(type.name()).anonymize(null), type.name());
        }
    }

    @Test
    public void testRoleIsAnonymized() {
        assertEquals(new DefaultAuthenticationRoleLoggingAnonymizer("NONE").anonymize("role"), "role");
        assertEquals(new DefaultAuthenticationRoleLoggingAnonymizer("REDACTED").anonymize("role"), "[REDACTED]");
        assertEquals(new DefaultAuthenticationRoleLoggingAnonymizer("SHA256").anonymize("role").substring(0, 8),
                "SHA-256:");
    }
}

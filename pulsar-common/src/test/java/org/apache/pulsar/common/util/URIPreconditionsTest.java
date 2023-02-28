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

import static org.apache.pulsar.common.util.URIPreconditions.*;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Objects;

public class URIPreconditionsTest {

    @Test
    public void testCheckURI() {
        // normal
        checkURI("http://pulsar.apache.org", uri -> true);
        checkURI("http://pulsar.apache.org", uri -> Objects.equals(uri.getScheme(), "http"));
        // illegal
        try {
            checkURI("pulsar.apache.org", uri -> Objects.equals(uri.getScheme(), "http"));
            Assert.fail("Unexpected behaviour");
        } catch (IllegalArgumentException ex) {
            // Ok
        }
        try {
            checkURI("pulsar.apache.org", uri -> Objects.equals(uri.getScheme(), "http"), "oops");
            Assert.fail("Unexpected behaviour");
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals(ex.getMessage(), "oops");
        }
    }

    @Test
    public void testCheckURIIfPresent() {
        checkURIIfPresent(null, uri -> false);
        checkURIIfPresent("http://pulsar.apache.org", uri -> true);
        try {
            checkURIIfPresent("http/pulsar.apache.org", uri -> uri.getScheme() != null, "Error");
            Assert.fail("Unexpected behaviour");
        } catch (IllegalArgumentException ex) {
            // Ok
        }
    }
}

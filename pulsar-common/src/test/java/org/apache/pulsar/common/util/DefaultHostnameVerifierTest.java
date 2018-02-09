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
package org.apache.pulsar.common.util;

import org.apache.pulsar.common.util.ssl.DefaultHostnameVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultHostnameVerifierTest {

    @Test
    public void testDefaultHostVerifier() {
        Assert.assertTrue(DefaultHostnameVerifier.matchIdentity("pulsar", "pulsar", null, true));
        Assert.assertFalse(DefaultHostnameVerifier.matchIdentity("pulsar.com", "pulsar", null, true));
        Assert.assertTrue(DefaultHostnameVerifier.matchIdentity("pulsar-broker1.com", "pulsar*.com", null, true));
        //unmatched remainder: "1-broker." should not contain "."
        Assert.assertFalse(DefaultHostnameVerifier.matchIdentity("pulsar-broker1.com", "pulsar*com", null, true));
        Assert.assertFalse(DefaultHostnameVerifier.matchIdentity("pulsar.com", "*", null, true));
    }

}

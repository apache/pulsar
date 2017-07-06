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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.testng.annotations.Test;

public class LocalPolicesTest {

    @Test
    public void testLocalPolices() {
        LocalPolicies localPolicy0 = new LocalPolicies();
        LocalPolicies localPolicy1 = new LocalPolicies();
        List<String> boundaries0 = new ArrayList<>();
        List<String> boundaries1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            boundaries0.add(String.valueOf(i));
            boundaries0.add(String.valueOf(i));
            boundaries1.add(String.valueOf(i));
        }
        localPolicy0.bundles.setBoundaries(boundaries0);
        localPolicy0.bundles.setNumBundles(boundaries0.size() - 1);
        localPolicy1.bundles.setBoundaries(boundaries1);
        localPolicy1.bundles.setNumBundles(boundaries1.size() - 1);
        assertFalse(localPolicy0.equals(localPolicy1));
        assertFalse(localPolicy0.equals(new OldPolicies()));
        localPolicy1.bundles.setBoundaries(boundaries0);
        localPolicy1.bundles.setNumBundles(boundaries0.size() - 1);
        assertTrue(localPolicy0.equals(localPolicy1));
    }
}

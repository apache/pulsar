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

import static org.apache.pulsar.common.policies.data.PoliciesUtil.defaultBundle;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LocalPolicesTest {

    @Test
    public void testLocalPolices() {
        List<String> boundaries0 = new ArrayList<>();
        List<String> boundaries1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            boundaries0.add(String.valueOf(i));
            boundaries0.add(String.valueOf(i));
            boundaries1.add(String.valueOf(i));
        }
        LocalPolicies localPolicy0 = new LocalPolicies(BundlesData.builder()
                .boundaries(boundaries0)
                .numBundles(boundaries0.size() - 1)
                .build(), null, null);
        LocalPolicies localPolicy1 = new LocalPolicies(BundlesData.builder()
                .boundaries(boundaries1)
                .numBundles(boundaries1.size() - 1)
                .build(), null, null);

        assertNotEquals(localPolicy1, localPolicy0);
        assertNotEquals(new OldPolicies(), localPolicy0);

        localPolicy1 = new LocalPolicies(BundlesData.builder()
                .boundaries(boundaries0)
                .numBundles(boundaries0.size() - 1)
                .build(), null, null);
        assertEquals(localPolicy1, localPolicy0);
    }

    // only for test
    class MutableLocalPolicies {

        public BundlesData bundles;
        // bookie affinity group for bookie-isolation
        public BookieAffinityGroupData bookieAffinityGroup;
        // namespace anti-affinity-group
        public String namespaceAntiAffinityGroup;

        public MutableLocalPolicies() {
            bundles = defaultBundle();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(bundles, bookieAffinityGroup);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof LocalPolicies) {
                LocalPolicies other = (LocalPolicies) obj;
                return Objects.equal(bundles, other.bundles)
                        && Objects.equal(bookieAffinityGroup, other.bookieAffinityGroup)
                        && Objects.equal(namespaceAntiAffinityGroup, other.namespaceAntiAffinityGroup);
            }
            return false;
        }

    }

    // https://github.com/apache/pulsar/pull/9598
    @Test
    public void testMakeLocalPoliciesImmutableSerializationCompatibility() throws IOException {

        // no fields
        MutableLocalPolicies mutableLocalPolicies = new MutableLocalPolicies();
        LocalPolicies immutableLocalPolicies = new LocalPolicies();

        // serialize and deserialize
        byte[] data = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(mutableLocalPolicies);
        LocalPolicies mutableDeserializedPolicies = ObjectMapperFactory.getThreadLocal().readValue(data, LocalPolicies.class);

        Assert.assertEquals(mutableDeserializedPolicies,immutableLocalPolicies);




        // check with set other fields
        BookieAffinityGroupData bookieAffinityGroupData = BookieAffinityGroupData.builder()
                .bookkeeperAffinityGroupPrimary("aaa")
                .bookkeeperAffinityGroupSecondary("bbb")
                .build();
        String namespaceAntiAffinityGroup = "namespace1,namespace2";

        mutableLocalPolicies.bookieAffinityGroup = bookieAffinityGroupData;
        mutableLocalPolicies.namespaceAntiAffinityGroup = namespaceAntiAffinityGroup;
        LocalPolicies immutableLocalPolicies2 = new LocalPolicies(defaultBundle(),bookieAffinityGroupData,namespaceAntiAffinityGroup);

        // serialize and deserialize
        data = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(mutableLocalPolicies);
        mutableDeserializedPolicies = ObjectMapperFactory.getThreadLocal().readValue(data, LocalPolicies.class);

        Assert.assertEquals(mutableDeserializedPolicies,immutableLocalPolicies2);

    }

    @Test
    public void testMakeLocalPoliciesImmutableStringSerializationCompatibility() throws IOException {

        // no fields
        MutableLocalPolicies mutableLocalPolicies = new MutableLocalPolicies();
        LocalPolicies immutableLocalPolicies = new LocalPolicies();

        // serialize and deserialize
        String data = ObjectMapperFactory.getThreadLocal().writeValueAsString(mutableLocalPolicies);
        LocalPolicies mutableDeserializedPolicies = ObjectMapperFactory.getThreadLocal().readValue(data, LocalPolicies.class);

        Assert.assertEquals(mutableDeserializedPolicies,immutableLocalPolicies);




        // check with set other fields
        BookieAffinityGroupData bookieAffinityGroupData = BookieAffinityGroupData.builder()
                .bookkeeperAffinityGroupPrimary("aaa")
                .bookkeeperAffinityGroupSecondary("bbb")
                .build();
        String namespaceAntiAffinityGroup = "namespace1,namespace2";

        mutableLocalPolicies.bookieAffinityGroup = bookieAffinityGroupData;
        mutableLocalPolicies.namespaceAntiAffinityGroup = namespaceAntiAffinityGroup;
        LocalPolicies immutableLocalPolicies2 = new LocalPolicies(defaultBundle(),bookieAffinityGroupData,namespaceAntiAffinityGroup);

        // serialize and deserialize
        data = ObjectMapperFactory.getThreadLocal().writeValueAsString(mutableLocalPolicies);
        mutableDeserializedPolicies = ObjectMapperFactory.getThreadLocal().readValue(data, LocalPolicies.class);

        Assert.assertEquals(mutableDeserializedPolicies,immutableLocalPolicies2);

    }
}

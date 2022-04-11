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
package org.apache.pulsar.broker.namespace;

import static org.testng.Assert.assertEquals;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceEphemeralDataTest {

    private static void setFields(NamespaceEphemeralData ned) throws Exception {
        ned.setNativeUrl("pulsar://localhost:6650");
        ned.setNativeUrlTls("pulsar+ssl://localhost:6651");
        ned.setHttpUrl("http://localhost:8080");
        ned.setHttpUrlTls("https://localhost:8443");
        ned.setDisabled(false);

        Map<String, AdvertisedListener> advertisedListeners = new HashMap<>();
        advertisedListeners.put("test-listener", AdvertisedListener.builder()
                .brokerServiceUrl(new URI("pulsar://adv-addr:6650"))
                .brokerServiceUrlTls(new URI("pulsar+ssl://adv-addr:6651"))
                .build());
        ned.setAdvertisedListeners(advertisedListeners);
    }

    /**
     * We must ensure NamespaceEphemeralData respect the equals() properties because it's used as a resource lock,
     * where equality is checked in the revalidation phase.
     */
    @Test
    public void testEquals() throws Exception {
        NamespaceEphemeralData ned1 = new NamespaceEphemeralData();
        setFields(ned1);

        NamespaceEphemeralData ned2 = new NamespaceEphemeralData();
        setFields(ned2);

        assertEquals(ned1.hashCode(), ned2.hashCode());
        assertEquals(ned1, ned2);
    }

}

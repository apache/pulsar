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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import com.google.common.io.Resources;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TrustManagerProxyTest {
    @DataProvider(name = "caDataProvider")
    public static Object[][] caDataProvider() {
        return new Object[][]{
                {"ca/multiple-ca.pem", 2},
                {"ca/single-ca.pem", 1}
        };
    }

    @Test(dataProvider = "caDataProvider")
    public void testLoadCA(String path, int count) {
        String caPath = Resources.getResource(path).getPath();

        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            TrustManagerProxy trustManagerProxy =
                    new TrustManagerProxy(caPath, 120, scheduledExecutor);
            X509Certificate[] x509Certificates = trustManagerProxy.getAcceptedIssuers();
            assertNotNull(x509Certificates);
            assertEquals(Arrays.stream(x509Certificates).count(), count);
        } finally {
            scheduledExecutor.shutdown();
        }
    }
}

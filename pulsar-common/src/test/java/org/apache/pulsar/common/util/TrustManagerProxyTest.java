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

import static org.testng.Assert.assertEquals;
import com.google.common.io.Resources;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TrustManagerProxyTest {

    @DataProvider(name = "certDataProvider")
    public static Object[][] caDataProvider() {
        return new Object[][]{
                {"ca/multiple-ca.pem", 2},
                {"ca/single-ca.pem", 1}
        };
    }

    @Test(dataProvider = "certDataProvider")
    public void testLoadCA(String path, int certCount) throws Exception {
        String certFilePath = Resources.getResource(path).getPath().replaceFirst("^/", "");
        @Cleanup("shutdownNow")
        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        TrustManagerProxy trustManagerProxy = new TrustManagerProxy(certFilePath, 60, scheduledExecutor);
        assertEquals(trustManagerProxy.getAcceptedIssuers().length, certCount);
    }
}

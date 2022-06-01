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
package org.apache.pulsar.functions.worker;

import static org.testng.Assert.assertEquals;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.testng.annotations.Test;

public class WorkerUtilsTest {


    @Test
    public void testDLogConfiguration() throws URISyntaxException, IOException {
        // The config yml is seeded with a fake bookie config.
        URL yamlUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig config = WorkerConfig.load(yamlUrl.toURI().getPath());

        // Map the config.
        DistributedLogConfiguration dlogConf = WorkerUtils.getDlogConf(config);

        // Verify the outcome.
        assertEquals(dlogConf.getString("bkc.testKey"), "fakeValue",
                "The bookkeeper client config mapping should apply.");
    }
}

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
package org.apache.pulsar.broker.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;
import java.util.List;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.model.ModelValidationException;
import org.testng.annotations.Test;

/**
 * Validates the Jersey resource models of the broker's REST resources, which {@link WebService} doesn't validate when
 * the broker starts: a model error that Jersey would fail the start for, like two resource methods for the same
 * requests, fails this test instead.
 */
@Test(groups = "broker")
public class WebServiceResourceModelTest extends SharedPulsarBaseTest {

    @Test
    public void testBrokerResourceModelsAreValid() {
        List<ResourceConfig> resourceConfigs = getPulsar().getWebService().getResourceConfigs();
        assertThat(resourceConfigs).isNotEmpty();
        for (ResourceConfig resourceConfig : resourceConfigs) {
            ResourceConfig validatedConfig = new ResourceConfig(resourceConfig)
                    .property(ServerProperties.RESOURCE_VALIDATION_DISABLE, false);
            try {
                new ApplicationHandler(validatedConfig);
            } catch (ModelValidationException e) {
                fail("Invalid resource model of " + resourceConfig.getClasses() + ": " + e.getIssues().stream()
                        .map(issue -> issue.getSeverity() + " " + issue.getMessage()).toList(), e);
            }
        }
    }
}

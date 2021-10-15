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
package org.apache.pulsar.broker.web;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * A base class for testing subclasses of {@link PulsarWebResource}.
 */
public abstract class PulsarWebResourceTest extends JerseyTestNg.ContainerPerClassTest {

    protected ServiceConfiguration config;
    protected PulsarService pulsar;

    protected PulsarWebResourceTest() {
        config = new ServiceConfiguration();

        pulsar = mock(PulsarService.class);
        doReturn(config).when(pulsar).getConfig();
        doReturn(config).when(pulsar).getConfiguration();

        set(TestProperties.CONTAINER_PORT, 0);
    }

    @BeforeClass(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Creates a JAX-RS resource configuration for test purposes.
     */
    @Override
    protected abstract ResourceConfig configure();

    /**
     * Creates a test container factory with servlet support.
     */
    @Override
    protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
        return new GrizzlyWebTestContainerFactory();
    }

    /**
     * Configures a deployment context for JAX-RS.
     */
    @Override
    protected DeploymentContext configureDeployment() {
        ResourceConfig app = configure();
        app.register(new Feature() {
            @Context
            ServletContext servletContext;

            @Override
            public boolean configure(FeatureContext context) {
                servletContext.setAttribute(WebService.ATTRIBUTE_PULSAR_NAME, pulsar);
                return true;
            }
        });
        return ServletDeploymentContext.forServlet(new ServletContainer(app)).build();
    }
}

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
package org.apache.pulsar.client.admin.internal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import javax.ws.rs.client.WebTarget;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.SinkConfig;
import org.testng.annotations.Test;

/**
 * Unit tests
 */
public class SinksImplTest {

    @Test
    public void testBadName() throws Exception {
        testBadConfig("tenant", "ns", "", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("sink name is required"));
        });
        testBadConfig("tenant", "ns", "     ", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("sink name is required"));
        });
        testBadConfig("tenant", "ns", null, err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("sink name is required"));
        });
    }

    @Test
    public void testBadTenant() throws Exception {
        testBadConfig("", "ns", "sink", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("tenant is required"));
        });
        testBadConfig("    ", "ns", "sink", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("tenant is required"));
        });
        testBadConfig(null, "ns", "sink", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("tenant is required"));
        });
    }

    @Test
    public void testBadNamespace() throws Exception {
        testBadConfig("tenant", "", "sink", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("namespace is required"));
        });
        testBadConfig("tenant", "    ", "sink", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("namespace is required"));
        });
        testBadConfig("tenant", null, "sink", err -> {
            assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("namespace is required"));
        });
    }

    private void testBadConfig(String tenant, String namespace, String sinkname,
            Consumer<ExecutionException> handler) throws Exception {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName(sinkname);
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        WebTarget web = mock(WebTarget.class);

        SinksImpl instance = new SinksImpl(web, null, null, 0);
        try {
            instance.createSinkAsync(sinkConfig, "").get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.getSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.getSinkStatusAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.getSinkStatusAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName(), 0).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.deleteSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.createSinkAsync(sinkConfig, "file.nar").get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.createSinkWithUrlAsync(sinkConfig, "http://localhost").get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.updateSinkAsync(sinkConfig, "file.nar", new UpdateOptionsImpl()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.updateSinkWithUrlAsync(sinkConfig, "http://localhost", new UpdateOptionsImpl()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.restartSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName(), 0).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.restartSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.stopSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName(), 0).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.stopSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.startSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName(), 0).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        try {
            instance.startSinkAsync(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName()).get();
            fail();
        } catch (ExecutionException err) {
            handler.accept(err);
        }

        // this function is only about namespace and tenant
        if (!StringUtils.isBlank(sinkname)) {
            try {
                instance.listSinksAsync(sinkConfig.getTenant(), sinkConfig.getNamespace()).get();
                fail();
            } catch (ExecutionException err) {
                handler.accept(err);
            }
        }

    }
}

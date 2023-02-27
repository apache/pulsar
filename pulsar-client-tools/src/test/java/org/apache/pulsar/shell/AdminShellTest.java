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
package org.apache.pulsar.shell;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import java.util.Properties;
import org.apache.pulsar.admin.cli.PulsarAdminSupplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.Topics;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AdminShellTest {

    private final Properties props = new Properties();
    private AdminShell adminShell;

    @BeforeMethod
    public void before() throws Exception {
        props.setProperty("webServiceUrl", "http://localhost:8080");
        adminShell = new AdminShell(props);
    }

    @Test
    public void test() throws Exception {
        final PulsarAdminBuilder builder = mock(PulsarAdminBuilder.class);
        final PulsarAdmin admin = mock(PulsarAdmin.class);
        when(builder.build()).thenReturn(admin);
        when(admin.topics()).thenReturn(mock(Topics.class));
        adminShell.setPulsarAdminSupplier(new PulsarAdminSupplier(builder, adminShell.getRootParams()));
        assertTrue(run(new String[]{"topics", "list", "public/default"}));
        verify(builder).build();
        assertTrue(run(new String[]{"topics", "list", "public/default"}));
        verify(builder).build();
        assertTrue(run(new String[]{"--admin-url", "http://localhost:8081",
                "topics", "list", "public/default"}));
        assertTrue(run(new String[]{"topics", "list", "public/default"}));
        verify(builder, times(3)).build();
        assertTrue(run(new String[]{"--admin-url", "http://localhost:8080",
                "topics", "list", "public/default"}));
        verify(builder, times(3)).build();
    }

    private boolean run(String[] args) throws Exception {
        try {
            return adminShell.runCommand(args);
        } finally {
            adminShell.cleanupState(props);
        }
    }
}
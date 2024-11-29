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
package org.apache.pulsar.admin.cli;

import com.google.common.collect.Lists;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCmdNamespaces {

    private PulsarAdmin pulsarAdmin;
    private CmdNamespaces cmdNamespaces;
    private Namespaces namespaces;

    @BeforeMethod
    public void setup() throws Exception {
        namespaces = mock(Namespaces.class);
        pulsarAdmin = mock(PulsarAdmin.class);
        when(pulsarAdmin.namespaces()).thenReturn(namespaces);
        cmdNamespaces = new CmdNamespaces(() -> pulsarAdmin);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws IOException {
        //NOTHING FOR NOW
    }

    @Test
    public void testListCmd() throws Exception {
        List<String> namespaceList = Lists.newArrayList("namespace1", "namespace2", "namespace3");
        doReturn(namespaceList).when(namespaces).getNamespaces(anyString());
        @Cleanup
        StringWriter stringWriter = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(stringWriter);
        cmdNamespaces.getCommander().setOut(printWriter);
        cmdNamespaces.run("list tenant1".split("\\s+"));
        Assert.assertEquals(stringWriter.toString(), String.join("\n", namespaceList) + "\n");
    }

    @Test
    public void testCreateCmd() throws Exception {
        ArgumentCaptor<String> namespaceCapture = ArgumentCaptor.forClass(String.class);
        doNothing().when(namespaces).createNamespace(namespaceCapture.capture(), any(Policies.class));
        cmdNamespaces.run("create tenant1/namespace1".split("\\s+"));
        verify(namespaces, times(1))
                .createNamespace(anyString(), any(Policies.class));
        Assert.assertEquals(namespaceCapture.getValue(), "tenant1/namespace1");
    }

    @Test
    public void testDeleteCmd() throws Exception {
        ArgumentCaptor<String> namespaceCapture = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Boolean> forceCapture = ArgumentCaptor.forClass(Boolean.class);
        doNothing().when(namespaces).deleteNamespace(namespaceCapture.capture(), forceCapture.capture());
        cmdNamespaces.run("delete tenant1/namespace1".split("\\s+"));
        verify(namespaces, times(1))
                .deleteNamespace(namespaceCapture.getValue(), forceCapture.getValue());
        Assert.assertEquals(namespaceCapture.getValue(), "tenant1/namespace1");
        Assert.assertEquals(forceCapture.getValue(), false);
    }

    @Test
    public void testSetRetentionCmd() throws Exception {
        cmdNamespaces.run("set-retention public/default -s 2T -t 2h".split("\\s+"));
        verify(namespaces, times(1)).setRetention("public/default", new RetentionPolicies(120, 2 * 1024 * 1024));
    }
}

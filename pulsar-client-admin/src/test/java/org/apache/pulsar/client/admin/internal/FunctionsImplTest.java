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
package org.apache.pulsar.client.admin.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.FunctionStatusSummary;
import org.testng.annotations.Test;

public class FunctionsImplTest {

    @Test
    public void testGetFunctionsWithStatusAsyncBuildsExpectedPath() throws Exception {
        WebTarget root = mock(WebTarget.class);
        WebTarget adminV3Functions = mock(WebTarget.class);
        WebTarget tenantTarget = mock(WebTarget.class);
        WebTarget namespaceTarget = mock(WebTarget.class);
        WebTarget statusTarget = mock(WebTarget.class);
        WebTarget summaryTarget = mock(WebTarget.class);

        when(root.path("/admin/v3/functions")).thenReturn(adminV3Functions);
        when(adminV3Functions.path("tenant-a")).thenReturn(tenantTarget);
        when(tenantTarget.path("namespace-a")).thenReturn(namespaceTarget);
        when(namespaceTarget.path("status")).thenReturn(statusTarget);
        when(statusTarget.path("summary")).thenReturn(summaryTarget);

        FunctionsImpl functions = org.mockito.Mockito.spy(new FunctionsImpl(root, null, null, 0));
        List<FunctionStatusSummary> expected = Collections.singletonList(
                FunctionStatusSummary.builder().name("fn-1").state(FunctionStatusSummary.SummaryState.RUNNING).build());
        CompletableFuture<List<FunctionStatusSummary>> response = CompletableFuture.completedFuture(expected);
        doReturn(response).when(functions).asyncGetRequest(eq(summaryTarget), any(GenericType.class));

        List<FunctionStatusSummary> actual =
                functions.getFunctionsWithStatusAsync("tenant-a", "namespace-a").get();

        verify(adminV3Functions).path("tenant-a");
        verify(tenantTarget).path("namespace-a");
        verify(namespaceTarget).path("status");
        verify(statusTarget).path("summary");
        assertEquals(actual, expected);
    }

    @Test
    public void testGetFunctionsWithStatusSyncDelegatesToAsync() throws Exception {
        WebTarget root = mock(WebTarget.class);
        WebTarget adminV3Functions = mock(WebTarget.class);
        WebTarget tenantTarget = mock(WebTarget.class);
        WebTarget namespaceTarget = mock(WebTarget.class);
        WebTarget statusTarget = mock(WebTarget.class);
        WebTarget summaryTarget = mock(WebTarget.class);

        when(root.path("/admin/v3/functions")).thenReturn(adminV3Functions);
        when(adminV3Functions.path("tenant-b")).thenReturn(tenantTarget);
        when(tenantTarget.path("namespace-b")).thenReturn(namespaceTarget);
        when(namespaceTarget.path("status")).thenReturn(statusTarget);
        when(statusTarget.path("summary")).thenReturn(summaryTarget);

        FunctionsImpl functions = org.mockito.Mockito.spy(new FunctionsImpl(root, null, null, 0));
        List<FunctionStatusSummary> expected = Collections.singletonList(
                FunctionStatusSummary.builder().name("fn-2").state(FunctionStatusSummary.SummaryState.STOPPED).build());
        CompletableFuture<List<FunctionStatusSummary>> response = CompletableFuture.completedFuture(expected);
        doReturn(response).when(functions).asyncGetRequest(eq(summaryTarget), any(GenericType.class));

        List<FunctionStatusSummary> actual = functions.getFunctionsWithStatus("tenant-b", "namespace-b");
        assertEquals(actual, expected);
    }

    @Test
    public void testGetFunctionsWithStatusAsyncFallsBackForLegacyBroker() throws Exception {
        WebTarget root = mock(WebTarget.class);
        WebTarget adminV3Functions = mock(WebTarget.class);
        WebTarget tenantTarget = mock(WebTarget.class);
        WebTarget namespaceTarget = mock(WebTarget.class);
        WebTarget statusTarget = mock(WebTarget.class);
        WebTarget summaryTarget = mock(WebTarget.class);

        when(root.path("/admin/v3/functions")).thenReturn(adminV3Functions);
        when(adminV3Functions.path("tenant-c")).thenReturn(tenantTarget);
        when(tenantTarget.path("namespace-c")).thenReturn(namespaceTarget);
        when(namespaceTarget.path("status")).thenReturn(statusTarget);
        when(statusTarget.path("summary")).thenReturn(summaryTarget);

        FunctionsImpl functions = org.mockito.Mockito.spy(new FunctionsImpl(root, null, null, 0));
        CompletableFuture<List<FunctionStatusSummary>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new PulsarAdminException.NotFoundException(null, "Not Found", 404));
        doReturn(failed).when(functions).asyncGetRequest(eq(summaryTarget), any(GenericType.class));
        doReturn(CompletableFuture.completedFuture(List.of("fn-b", "fn-a")))
                .when(functions).getFunctionsAsync("tenant-c", "namespace-c");

        FunctionStatus runningStatus = new FunctionStatus();
        runningStatus.setNumInstances(1);
        runningStatus.setNumRunning(1);
        FunctionStatus stoppedStatus = new FunctionStatus();
        stoppedStatus.setNumInstances(1);
        stoppedStatus.setNumRunning(0);
        doReturn(CompletableFuture.completedFuture(stoppedStatus))
                .when(functions).getFunctionStatusAsync("tenant-c", "namespace-c", "fn-b");
        doReturn(CompletableFuture.completedFuture(runningStatus))
                .when(functions).getFunctionStatusAsync("tenant-c", "namespace-c", "fn-a");

        List<FunctionStatusSummary> actual =
                functions.getFunctionsWithStatusAsync("tenant-c", "namespace-c").get();

        assertEquals(actual.size(), 2);
        assertEquals(actual.get(0).getName(), "fn-a");
        assertEquals(actual.get(0).getState(), FunctionStatusSummary.SummaryState.RUNNING);
        assertEquals(actual.get(1).getName(), "fn-b");
        assertEquals(actual.get(1).getState(), FunctionStatusSummary.SummaryState.STOPPED);
    }

    @Test
    public void testGetFunctionsWithStatusAsyncFallbackPagesBeforeQueryingStatus() throws Exception {
        WebTarget root = mock(WebTarget.class);
        WebTarget adminV3Functions = mock(WebTarget.class);
        WebTarget tenantTarget = mock(WebTarget.class);
        WebTarget namespaceTarget = mock(WebTarget.class);
        WebTarget statusTarget = mock(WebTarget.class);
        WebTarget summaryTarget = mock(WebTarget.class);
        WebTarget limitTarget = mock(WebTarget.class);
        WebTarget continuationTarget = mock(WebTarget.class);

        when(root.path("/admin/v3/functions")).thenReturn(adminV3Functions);
        when(adminV3Functions.path("tenant-d")).thenReturn(tenantTarget);
        when(tenantTarget.path("namespace-d")).thenReturn(namespaceTarget);
        when(namespaceTarget.path("status")).thenReturn(statusTarget);
        when(statusTarget.path("summary")).thenReturn(summaryTarget);
        when(summaryTarget.queryParam("limit", 1)).thenReturn(limitTarget);
        when(limitTarget.queryParam("continuationToken", "fn-a")).thenReturn(continuationTarget);

        FunctionsImpl functions = org.mockito.Mockito.spy(new FunctionsImpl(root, null, null, 0));
        CompletableFuture<List<FunctionStatusSummary>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new PulsarAdminException.NotFoundException(null, "Not Found", 404));
        doReturn(failed).when(functions).asyncGetRequest(eq(continuationTarget), any(GenericType.class));
        doReturn(CompletableFuture.completedFuture(List.of("fn-c", "fn-a", "fn-b")))
                .when(functions).getFunctionsAsync("tenant-d", "namespace-d");

        FunctionStatus runningStatus = new FunctionStatus();
        runningStatus.setNumInstances(1);
        runningStatus.setNumRunning(1);
        doReturn(CompletableFuture.completedFuture(runningStatus))
                .when(functions).getFunctionStatusAsync("tenant-d", "namespace-d", "fn-b");

        List<FunctionStatusSummary> actual =
                functions.getFunctionsWithStatusAsync("tenant-d", "namespace-d", 1, "fn-a").get();

        assertEquals(actual.size(), 1);
        assertEquals(actual.get(0).getName(), "fn-b");
        verify(functions).getFunctionStatusAsync("tenant-d", "namespace-d", "fn-b");
        verify(functions, never()).getFunctionStatusAsync("tenant-d", "namespace-d", "fn-a");
        verify(functions, never()).getFunctionStatusAsync("tenant-d", "namespace-d", "fn-c");
    }
}

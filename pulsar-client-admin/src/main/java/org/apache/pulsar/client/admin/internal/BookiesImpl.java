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

import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.Bookies;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;

public class BookiesImpl extends BaseResource implements Bookies {
    private final WebTarget adminBookies;

    public BookiesImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminBookies = web.path("/admin/v2/bookies");
    }

    @Override
    public BookiesRackConfiguration getBookiesRackInfo() throws PulsarAdminException {
        return sync(() -> getBookiesRackInfoAsync());
    }

    @Override
    public CompletableFuture<BookiesClusterInfo> getBookiesAsync() {
        WebTarget path = adminBookies.path("all");
        final CompletableFuture<BookiesClusterInfo> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BookiesClusterInfo>() {
                    @Override
                    public void completed(BookiesClusterInfo bookies) {
                        future.complete(bookies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public BookiesClusterInfo getBookies() throws PulsarAdminException {
        return sync(() -> getBookiesAsync());
    }

    @Override
    public CompletableFuture<BookiesRackConfiguration> getBookiesRackInfoAsync() {
        WebTarget path = adminBookies.path("racks-info");
        final CompletableFuture<BookiesRackConfiguration> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BookiesRackConfiguration>() {
                    @Override
                    public void completed(BookiesRackConfiguration bookiesRackConfiguration) {
                        future.complete(bookiesRackConfiguration);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public BookieInfo getBookieRackInfo(String bookieAddress) throws PulsarAdminException {
        return sync(() -> getBookieRackInfoAsync(bookieAddress));
    }

    @Override
    public CompletableFuture<BookieInfo> getBookieRackInfoAsync(String bookieAddress) {
        WebTarget path = adminBookies.path("racks-info").path(bookieAddress);
        final CompletableFuture<BookieInfo> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BookieInfo>() {
                    @Override
                    public void completed(BookieInfo bookieInfo) {
                        future.complete(bookieInfo);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void deleteBookieRackInfo(String bookieAddress) throws PulsarAdminException {
        sync(() -> deleteBookieRackInfoAsync(bookieAddress));
    }

    @Override
    public CompletableFuture<Void> deleteBookieRackInfoAsync(String bookieAddress) {
        WebTarget path = adminBookies.path("racks-info").path(bookieAddress);
        return asyncDeleteRequest(path);
    }

    @Override
    public void updateBookieRackInfo(String bookieAddress, String group, BookieInfo bookieInfo)
            throws PulsarAdminException {
        sync(() -> updateBookieRackInfoAsync(bookieAddress, group, bookieInfo));
    }

    @Override
    public CompletableFuture<Void> updateBookieRackInfoAsync(
            String bookieAddress, String group, BookieInfo bookieInfo) {
        WebTarget path = adminBookies.path("racks-info").path(bookieAddress).queryParam("group", group);
        return asyncPostRequest(path, Entity.entity(bookieInfo, MediaType.APPLICATION_JSON));
    }

}

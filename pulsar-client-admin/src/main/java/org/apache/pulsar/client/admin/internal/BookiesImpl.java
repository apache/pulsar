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

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.Bookies;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.ErrorData;

public class BookiesImpl extends BaseResource implements Bookies {
    private final WebTarget adminBookies;

    public BookiesImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminBookies = web.path("/admin/v2/bookies");
    }

    @Override
    public BookiesRackConfiguration getBookiesRackInfo() throws PulsarAdminException {
        try {
            return request(adminBookies.path("racks-info")).get(BookiesRackConfiguration.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public BookieInfo getBookieRackInfo(String bookieAddress) throws PulsarAdminException {
        try {
            return request(adminBookies.path("racks-info").path(bookieAddress)).get(BookieInfo.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteBookieRackInfo(String bookieAddress) throws PulsarAdminException {
        try {
            request(adminBookies.path("racks-info").path(bookieAddress)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateBookieRackInfo(String bookieAddress, String group, BookieInfo bookieInfo)
            throws PulsarAdminException {
        try {
            request(adminBookies.path("racks-info").path(bookieAddress).queryParam("group", group))
                    .post(Entity.entity(bookieInfo, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}

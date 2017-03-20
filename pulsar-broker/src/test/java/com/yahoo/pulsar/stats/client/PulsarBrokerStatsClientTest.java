/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.stats.client;

import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.net.URL;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.ConflictException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.NotFoundException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.ServerSideErrorException;
import com.yahoo.pulsar.client.admin.internal.BrokerStatsImpl;
import com.yahoo.pulsar.client.api.Authentication;

public class PulsarBrokerStatsClientTest {

    @Test
    public void testServiceException() throws Exception {
        URL url = new URL("http://localhost:15000");
		PulsarAdmin admin = new PulsarAdmin(url, (Authentication) null);
		BrokerStatsImpl client = (BrokerStatsImpl) spy(admin.brokerStats());
        try {
            client.getLoadReport();
        } catch (PulsarAdminException e) {
            // Ok
        }
        try {
            client.getPendingBookieOpsStats();
        } catch (PulsarAdminException e) {
            // Ok
        }
        try {
            client.getBrokerResourceAvailability("prop", "cluster", "ns");
        } catch (PulsarAdminException e) {
            // Ok
        }
        assertTrue(client.getApiException(new ClientErrorException(403)) instanceof NotAuthorizedException);
        assertTrue(client.getApiException(new ClientErrorException(404)) instanceof NotFoundException);
        assertTrue(client.getApiException(new ClientErrorException(409)) instanceof ConflictException);
        assertTrue(client.getApiException(new ClientErrorException(412)) instanceof PreconditionFailedException);
        assertTrue(client.getApiException(new ClientErrorException(400)) instanceof PulsarAdminException);
        assertTrue(client.getApiException(new ServerErrorException(500)) instanceof ServerSideErrorException);
        assertTrue(client.getApiException(new ServerErrorException(503)) instanceof PulsarAdminException);

        log.info("Client: ", client);

        admin.close();
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStatsClientTest.class);
}

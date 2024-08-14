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
package org.apache.pulsar.broker.admin;

import io.jsonwebtoken.Jwts;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.mockito.Mockito.doReturn;

public class AuthZTest extends MockedPulsarStandalone {

    protected PulsarAdmin superUserAdmin;

    protected PulsarAdmin tenantManagerAdmin;

    protected AuthorizationService authorizationService;

    protected AuthorizationService orignalAuthorizationService;

    protected static final String TENANT_ADMIN_SUBJECT =  UUID.randomUUID().toString();
    protected static final String TENANT_ADMIN_TOKEN = Jwts.builder()
            .claim("sub", TENANT_ADMIN_SUBJECT).signWith(SECRET_KEY).compact();

    @Override
    public void close() throws Exception {
        if (superUserAdmin != null) {
            superUserAdmin.close();
            superUserAdmin = null;
        }
        if (tenantManagerAdmin != null) {
            tenantManagerAdmin.close();
            tenantManagerAdmin = null;
        }
        authorizationService = null;
        orignalAuthorizationService = null;
        super.close();
    }

    @BeforeMethod(alwaysRun = true)
    public void before() throws IllegalAccessException {
        orignalAuthorizationService = getPulsarService().getBrokerService().getAuthorizationService();
        authorizationService = Mockito.spy(orignalAuthorizationService);
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                authorizationService, true);
    }

    @AfterMethod(alwaysRun = true)
    public void after() throws IllegalAccessException {
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                orignalAuthorizationService, true);
    }

    protected AtomicBoolean setAuthorizationTopicOperationChecker(String role, Object operation) {
        AtomicBoolean execFlag = new AtomicBoolean(false);
        if (operation instanceof TopicOperation) {
            Mockito.doAnswer(invocationOnMock -> {
                String role_ = invocationOnMock.getArgument(2);
                if (role.equals(role_)) {
                    TopicOperation operation_ = invocationOnMock.getArgument(1);
                    Assert.assertEquals(operation_, operation);
                }
                execFlag.set(true);
                return invocationOnMock.callRealMethod();
            }).when(authorizationService).allowTopicOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any());
        } else if (operation instanceof NamespaceOperation) {
            doReturn(true)
                    .when(authorizationService).isValidOriginalPrincipal(Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.doAnswer(invocationOnMock -> {
                String role_ = invocationOnMock.getArgument(2);
                if (role.equals(role_)) {
                    TopicOperation operation_ = invocationOnMock.getArgument(1);
                    Assert.assertEquals(operation_, operation);
                }
                execFlag.set(true);
                return invocationOnMock.callRealMethod();
            }).when(authorizationService).allowNamespaceOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any());
        } else {
            throw new IllegalArgumentException("");
        }


        return execFlag;
    }

    protected void createTopic(String topic, boolean partitioned) throws Exception {
        if (partitioned) {
            superUserAdmin.topics().createPartitionedTopic(topic, 2);
        } else {
            superUserAdmin.topics().createNonPartitionedTopic(topic);
        }
    }

    protected void deleteTopic(String topic, boolean partitioned) throws Exception {
        if (partitioned) {
            superUserAdmin.topics().deletePartitionedTopic(topic, true);
        } else {
            superUserAdmin.topics().delete(topic, true);
        }
    }
}

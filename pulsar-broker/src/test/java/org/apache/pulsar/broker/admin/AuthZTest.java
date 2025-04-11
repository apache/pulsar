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

import static org.mockito.Mockito.doReturn;
import io.jsonwebtoken.Jwts;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;

public abstract class AuthZTest extends MockedPulsarStandalone {

    protected PulsarAdmin superUserAdmin;

    protected PulsarAdmin tenantManagerAdmin;

    protected AuthorizationService authorizationService;

    protected static final String TENANT_ADMIN_SUBJECT =  UUID.randomUUID().toString();
    protected static final String TENANT_ADMIN_TOKEN = Jwts.builder()
            .claim("sub", TENANT_ADMIN_SUBJECT).signWith(SECRET_KEY).compact();

    private volatile Consumer<InvocationOnMock> allowTopicOperationAsyncHandler;
    private volatile Consumer<InvocationOnMock> allowNamespaceOperationAsyncHandler;

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
        if (authorizationService != null) {
            Mockito.reset(authorizationService);
            authorizationService = null;
        }
        super.close();
    }

    @SneakyThrows
    @Override
    protected void start() {
        super.start();
        authorizationService = BrokerTestUtil.spyWithoutRecordingInvocations(
                getPulsarService().getBrokerService().getAuthorizationService());
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                authorizationService, true);
        Mockito.doAnswer(invocationOnMock -> {
            Consumer<InvocationOnMock> localAllowTopicOperationAsyncHandler =
                    allowTopicOperationAsyncHandler;
            if (localAllowTopicOperationAsyncHandler != null) {
                localAllowTopicOperationAsyncHandler.accept(invocationOnMock);
            }
            return invocationOnMock.callRealMethod();
        }).when(authorizationService).allowTopicOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
        doReturn(true)
                .when(authorizationService).isValidOriginalPrincipal(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocationOnMock -> {
            Consumer<InvocationOnMock> localAllowNamespaceOperationAsyncHandler =
                    allowNamespaceOperationAsyncHandler;
            if (localAllowNamespaceOperationAsyncHandler != null) {
                localAllowNamespaceOperationAsyncHandler.accept(invocationOnMock);
            }
            return invocationOnMock.callRealMethod();
        }).when(authorizationService).allowNamespaceOperationAsync(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
    }

    @AfterMethod(alwaysRun = true)
    public void after() throws IllegalAccessException {
        allowNamespaceOperationAsyncHandler = null;
        allowTopicOperationAsyncHandler = null;
    }

    protected AtomicBoolean setAuthorizationTopicOperationChecker(String role, Object operation) {
        AtomicBoolean execFlag = new AtomicBoolean(false);
        if (operation instanceof TopicOperation) {
            allowTopicOperationAsyncHandler = invocationOnMock -> {
                String role_ = invocationOnMock.getArgument(2);
                if (role.equals(role_)) {
                    TopicOperation operation_ = invocationOnMock.getArgument(1);
                    Assert.assertEquals(operation_, operation);
                }
                execFlag.set(true);
            };
        } else if (operation instanceof NamespaceOperation) {
            allowNamespaceOperationAsyncHandler = invocationOnMock -> {
                String role_ = invocationOnMock.getArgument(2);
                if (role.equals(role_)) {
                    TopicOperation operation_ = invocationOnMock.getArgument(1);
                    Assert.assertEquals(operation_, operation);
                }
                execFlag.set(true);
            };
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

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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;

public class PulsarAdminSupplier implements Supplier<PulsarAdmin> {

    @Data
    private static final class RootParamsKey {
        String serviceUrl;
        String authPluginClassName;
        int requestTimeout;
        String authParams;
        Boolean tlsAllowInsecureConnection;
        String tlsTrustCertsFilePath;
        Boolean tlsEnableHostnameVerification;
        String tlsProvider;

        static RootParamsKey fromRootParams(PulsarAdminTool.RootParams params) {
            final RootParamsKey key = new RootParamsKey();
            key.setServiceUrl(params.getServiceUrl());
            key.setAuthParams(params.getAuthParams());
            key.setAuthPluginClassName(params.getAuthPluginClassName());
            key.setRequestTimeout(params.getRequestTimeout());
            key.setTlsAllowInsecureConnection(params.getTlsAllowInsecureConnection());
            key.setTlsTrustCertsFilePath(params.getTlsTrustCertsFilePath());
            key.setTlsEnableHostnameVerification(params.getTlsEnableHostnameVerification());
            key.setTlsProvider(params.getTlsProvider());
            return key;
        }
    }

    private final PulsarAdminBuilder adminBuilder;
    private RootParamsKey currentParamsKey;
    private PulsarAdmin admin;

    public PulsarAdminSupplier(PulsarAdminBuilder baseAdminBuilder, PulsarAdminTool.RootParams rootParams) {
        this.adminBuilder = baseAdminBuilder;
        rootParamsUpdated(rootParams);
    }

    @Override
    public PulsarAdmin get() {
        if (admin == null) {
            try {
                admin = adminBuilder.build();
            } catch (Exception ex) {
                System.err.println(ex.getClass() + ": " + ex.getMessage());
                throw new RuntimeException("Not able to create pulsar admin: " + ex.getMessage(), ex);
            }
        }
        return admin;
    }

    void rootParamsUpdated(PulsarAdminTool.RootParams newParams) {
        final RootParamsKey newParamsKey = RootParamsKey.fromRootParams(newParams);
        if (newParamsKey.equals(currentParamsKey)) {
            return;
        }
        applyRootParamsToAdminBuilder(adminBuilder, newParams);
        currentParamsKey = newParamsKey;
        if (admin != null) {
            admin.close();
        }
        this.admin = null;
    }

    @SneakyThrows
    private static void applyRootParamsToAdminBuilder(PulsarAdminBuilder adminBuilder,
                                                      PulsarAdminTool.RootParams rootParams) {
        adminBuilder.serviceHttpUrl(rootParams.serviceUrl);
        adminBuilder.authentication(rootParams.authPluginClassName, rootParams.authParams);
        adminBuilder.requestTimeout(rootParams.requestTimeout, TimeUnit.SECONDS);
        if (rootParams.tlsAllowInsecureConnection != null) {
            adminBuilder.allowTlsInsecureConnection(rootParams.tlsAllowInsecureConnection);
        }
        if (rootParams.tlsEnableHostnameVerification != null) {
            adminBuilder.enableTlsHostnameVerification(rootParams.tlsEnableHostnameVerification);
        }
        if (isNotBlank(rootParams.tlsProvider)) {
            adminBuilder.sslProvider(rootParams.tlsProvider);
        }
    }

}

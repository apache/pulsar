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
package org.apache.pulsar.broker.namespace;

import com.google.common.base.Objects;

public class NamespaceEphemeralData {
    private String nativeUrl;
    private String nativeUrlTls;
    private String httpUrl;
    private String httpUrlTls;
    private boolean disabled;

    public NamespaceEphemeralData() {
    }

    public NamespaceEphemeralData(String brokerUrl, String brokerUrlTls, String httpUrl, String httpUrlTls,
            boolean disabled) {
        this.nativeUrl = brokerUrl;
        this.nativeUrlTls = brokerUrlTls;
        this.httpUrl = httpUrl;
        this.httpUrlTls = httpUrlTls;
        this.disabled = disabled;
    }

    public String getNativeUrl() {
        return nativeUrl;
    }

    public String getNativeUrlTls() {
        return nativeUrlTls;
    }

    public String getHttpUrl() {
        return httpUrl;
    }

    public String getHttpUrlTls() {
        return httpUrlTls;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean flag) {
        this.disabled = flag;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("nativeUrl", nativeUrl).add("httpUrl", httpUrl)
                .add("disabled", disabled).toString();
    }
}

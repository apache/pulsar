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
package com.yahoo.pulsar.common.policies.data;

import com.google.common.base.Objects;

public class ClusterData {
    private String serviceUrl;
    private String serviceUrlTls;

    public ClusterData() {
    }

    public ClusterData(String serviceUrl) {
        this(serviceUrl, "");
    }

    public ClusterData(String serviceUrl, String serviceUrlTls) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public void setServiceUrlTls(String serviceUrlTls) {
        this.serviceUrlTls = serviceUrlTls;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClusterData) {
            ClusterData other = (ClusterData) obj;
            return Objects.equal(serviceUrl, other.serviceUrl) && Objects.equal(serviceUrlTls, other.serviceUrlTls);
        }

        return false;
    }

    @Override
    public int hashCode() {
        if (serviceUrlTls != null && !serviceUrlTls.isEmpty()) {
            return Objects.hashCode(serviceUrl + serviceUrlTls);
        } else {
            return Objects.hashCode(serviceUrl);
        }
    }

    @Override
    public String toString() {
        if (serviceUrlTls == null || serviceUrlTls.isEmpty()) {
            return serviceUrl;
        } else {
            return serviceUrl + "," + serviceUrlTls;
        }
    }
}

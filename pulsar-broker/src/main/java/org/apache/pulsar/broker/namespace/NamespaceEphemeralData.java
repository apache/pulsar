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

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;

@Data
@NoArgsConstructor
public class NamespaceEphemeralData {
    private String nativeUrl;
    private String nativeUrlTls;
    private String httpUrl;
    private String httpUrlTls;
    private boolean disabled;
    private Map<String, AdvertisedListener> advertisedListeners;

    public NamespaceEphemeralData(String brokerUrl, String brokerUrlTls, String httpUrl, String httpUrlTls,
            boolean disabled) {
        this(brokerUrl, brokerUrlTls, httpUrl, httpUrlTls, disabled, null);
    }

    public NamespaceEphemeralData(String brokerUrl, String brokerUrlTls, String httpUrl, String httpUrlTls,
                                  boolean disabled, Map<String, AdvertisedListener> advertisedListeners) {
        this.nativeUrl = brokerUrl;
        this.nativeUrlTls = brokerUrlTls;
        this.httpUrl = httpUrl;
        this.httpUrlTls = httpUrlTls;
        this.disabled = disabled;
        if (advertisedListeners == null) {
            this.advertisedListeners = Collections.EMPTY_MAP;
        } else {
            this.advertisedListeners = Maps.newHashMap(advertisedListeners);
        }
    }

    @NotNull
    public Map<String, AdvertisedListener> getAdvertisedListeners() {
        if (this.advertisedListeners == null) {
            return Collections.EMPTY_MAP;
        }
        return Collections.unmodifiableMap(this.advertisedListeners);
    }
}

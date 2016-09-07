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
package com.yahoo.pulsar.broker.lookup;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.URI;

import com.yahoo.pulsar.common.lookup.data.LookupData;
import com.yahoo.pulsar.broker.namespace.NamespaceEphemeralData;

/**
 * Represent a lookup result.
 *
 * Result can either be the lookup data describing the broker that owns the broker or the HTTP endpoint to where we need
 * to redirect the client to try again.
 */
public class LookupResult {
    enum Type {
        BrokerUrl, HttpRedirectUrl
    }

    private final Type type;
    private final LookupData lookupData;
    private final URI httpRedirectAddress;

    public LookupResult(NamespaceEphemeralData namespaceEphemeralData) {
        this.type = Type.BrokerUrl;
        this.lookupData = new LookupData(namespaceEphemeralData.getNativeUrl(),
                namespaceEphemeralData.getNativeUrlTls(), namespaceEphemeralData.getHttpUrl());
        this.httpRedirectAddress = null;
    }

    public LookupResult(URI httpRedirectAddress) {
        this.type = Type.HttpRedirectUrl;
        this.lookupData = null;
        this.httpRedirectAddress = httpRedirectAddress;
    }

    public boolean isBrokerUrl() {
        return type == Type.BrokerUrl;
    }

    public boolean isHttpRedirect() {
        return type == Type.HttpRedirectUrl;
    }

    public LookupData getLookupData() {
        checkArgument(isBrokerUrl());
        return lookupData;
    }

    public URI getHttpRedirectAddress() {
        checkArgument(isHttpRedirect());
        return httpRedirectAddress;
    }
}

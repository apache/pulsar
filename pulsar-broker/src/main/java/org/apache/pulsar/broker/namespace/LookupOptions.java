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

import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
@Builder
public class LookupOptions {
    /**
     * If authoritative, it means the lookup had already been redirected here by a different broker.
     */
    private final boolean authoritative;

    /**
     * If read-only, do not attempt to acquire ownership.
     */
    private final boolean readOnly;

    /**
     * After acquiring the ownership, load all the topics.
     */
    private final boolean loadTopicsInBundle;

    /**
     * The lookup request was made through HTTPs.
     */
    private final boolean requestHttps;

    private final String advertisedListenerName;

    public boolean hasAdvertisedListenerName() {
        return StringUtils.isNotBlank(advertisedListenerName);
    }
}

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

package org.apache.pulsar.common.lookup.data;

// avoid cyclic dependency
public interface LookupDataInterface {

    String getBrokerUrl();

    String getBrokerUrlTls();

    String getHttpUrl();

    String getHttpUrlTls();
    /**
     * Legacy name, but client libraries are still using it so it needs to be included in Json.
     */
    @Deprecated
    String getNativeUrl();
    /**
     * "brokerUrlSsl" is needed in the serialized Json for compatibility reasons.
     *
     * <p>Older C++ pulsar client library version will fail the lookup if this field is not included,
     * even though it's not used
     */
    @Deprecated
    String getBrokerUrlSsl();
}

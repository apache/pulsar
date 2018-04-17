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
package org.apache.pulsar.replicator.auth;

import java.util.Map;


/**
 * Simple Default AuthParamStore which stores auth-data into replicator-properties and retrieves from the same
 * replicator-properties which is part of the namespace-policies.
 * 
 *
 */
public class DefaultAuthParamKeyStore implements AuthParamKeyStore {

    /**
     * Merge auth-data into replicatorProperties which will be stored into namespace policies.
     */
    @Override
    public void storeAuthData(String namespace, Map<String, String> replicatorProperites, Map<String, String> authData)
            throws Exception {
        if (replicatorProperites!=null && authData != null) {
            replicatorProperites.putAll(authData);
        }
    }

    /**
     * Returns replicatorProperties that already has authData merged in it.
     */
    @Override
    public Map<String, String> fetchAuthData(String namespace, Map<String, String> replicatorProperites)
            throws Exception {
        return replicatorProperites;
    }

}

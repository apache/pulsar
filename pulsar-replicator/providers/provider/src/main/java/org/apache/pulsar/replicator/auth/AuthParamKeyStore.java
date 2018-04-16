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
 * Replicator connects to targeted external system which will require to pass
 * authentication param. Therefore, {@link AuthParamKeyStore} will be used to
 * store and get authentication-data-map from the appropriate Secured-KeyStore
 * System.
 * 
 */
public interface AuthParamKeyStore {

	/**
	 * AuthorizationKeyStore stores provided authData to appropriate KeyStore system
	 * 
	 * 
	 * @param namespace
	 * @param replicatorProperites
	 * @throws Exception
	 */
	void storeAuthData(String namespace, Map<String, String> replicatorProperites, Map<String, String> authData)
			throws Exception;

	/**
	 * 
	 * AuthorizationKeyStore uses replicator metadata information present into
	 * #replicatorProperites to fetch auth-data stored at appropriate KeyStore
	 * system.
	 * 
	 * @param namespace
	 * @param replicatorProperites
	 * @return
	 * @throws Exception
	 */
	Map<String, String> fetchAuthData(String namespace, Map<String, String> replicatorProperites) throws Exception;

}

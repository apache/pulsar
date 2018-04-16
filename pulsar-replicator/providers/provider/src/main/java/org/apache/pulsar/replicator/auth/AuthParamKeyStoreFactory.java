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

import java.lang.reflect.Constructor;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class AuthParamKeyStoreFactory {

	private static final Map<String, AuthParamKeyStore> authParamKeyStoreInstanceMap = Maps.newHashMap();

	public static synchronized AuthParamKeyStore create(String pluginFQClassName) throws IllegalArgumentException {
		if (authParamKeyStoreInstanceMap.containsKey(pluginFQClassName)) {
			return authParamKeyStoreInstanceMap.get(pluginFQClassName);
		}
		try {
			Class<?> clazz = Class.forName(pluginFQClassName);
			Constructor<?> ctor = clazz.getConstructor();
			return (AuthParamKeyStore) ctor.newInstance(new Object[] {});
		} catch (Exception e) {
			log.error("Failed to initialize AuthParamKeyStore for plugin {}", pluginFQClassName, e);
			throw new IllegalArgumentException(
					String.format("invalid authplugin name %s , failed to init %s", pluginFQClassName, e.getMessage()));
		}
	}

	private static final Logger log = LoggerFactory.getLogger(AuthParamKeyStoreFactory.class);

}

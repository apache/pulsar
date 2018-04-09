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
package org.apache.pulsar.client.api;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.common.util.ObjectMapperFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AuthenticationUtil {
	public static Map<String, String> configureFromJsonString(String authParamsString) throws IOException {
		ObjectMapper jsonMapper = ObjectMapperFactory.create();
		return jsonMapper.readValue(authParamsString, new TypeReference<HashMap<String, String>>() {
		});
	}

	public static Map<String, String> configureFromPulsar1AuthParamString(String authParamsString) {
		Map<String, String> authParams = new HashMap<>();

		if (isNotBlank(authParamsString)) {
			String[] params = authParamsString.split(",");
			for (String p : params) {
				String[] kv = p.split(":");
				if (kv.length == 2) {
					authParams.put(kv[0], kv[1]);
				}
			}
		}
		return authParams;
	}
}

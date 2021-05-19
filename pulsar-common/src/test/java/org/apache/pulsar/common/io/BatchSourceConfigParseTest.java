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
package org.apache.pulsar.common.io;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BatchSourceConfigParseTest {
	
	private ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public final void ImmediateTriggererTest() throws JsonMappingException, JsonProcessingException {
		String json = "{ \"discoveryTriggererClassName\" : \"org.apache.pulsar.io.batchdiscovery.ImmediateTriggerer\" }";
		BatchSourceConfig config = objectMapper.readValue(json, BatchSourceConfig.class);
		assertNotNull(config);
		assertEquals(config.getDiscoveryTriggererClassName(), "org.apache.pulsar.io.batchdiscovery.ImmediateTriggerer");
	}
	
	@Test
	public final void CronTriggererTest() throws JsonMappingException, JsonProcessingException {
		String json = "{ \"discoveryTriggererClassName\" : \"org.apache.pulsar.io.batchdiscovery.CronTriggerer\","
				+ "\"discoveryTriggererConfig\": {\"cron\": \"5 0 0 0 0 *\"} }";
		BatchSourceConfig config = objectMapper.readValue(json, BatchSourceConfig.class);
		assertNotNull(config);
		assertEquals(config.getDiscoveryTriggererClassName(), "org.apache.pulsar.io.batchdiscovery.CronTriggerer");
		assertNotNull(config.getDiscoveryTriggererConfig());
		assertEquals(config.getDiscoveryTriggererConfig().size(), 1);
		assertEquals(config.getDiscoveryTriggererConfig().get("cron"), "5 0 0 0 0 *");
	}
}

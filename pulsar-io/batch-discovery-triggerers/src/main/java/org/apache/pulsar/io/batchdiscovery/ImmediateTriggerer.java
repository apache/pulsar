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
package org.apache.pulsar.io.batchdiscovery;

import java.util.Map;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.BatchSourceTriggerer;
import org.apache.pulsar.io.core.SourceContext;

@Slf4j
public class ImmediateTriggerer implements BatchSourceTriggerer {
	
  @Override
  public void init(Map<String, Object> map, SourceContext sourceContext) throws Exception {
    log.info("Initialized ImmediateTrigger at: {}",  System.currentTimeMillis());
  }

  @Override
  public void start(Consumer<String> consumer) {
    consumer.accept("");
  }

  @Override
  public void stop() {

  }
}

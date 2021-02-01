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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.pulsar.io.core.BatchSourceTriggerer;
import org.apache.pulsar.io.core.SourceContext;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImmediateTriggerer implements BatchSourceTriggerer {
	
	private ThreadPoolTaskScheduler scheduler;

	@Override
	public void init(Map<String, Object> config, SourceContext sourceContext) throws Exception {
	  scheduler = new ThreadPoolTaskScheduler();
      scheduler.setThreadNamePrefix(String.format("%s/%s/%s-cron-triggerer-",
	      sourceContext.getTenant(), sourceContext.getNamespace(), sourceContext.getSourceName()));

	  log.info("Initialized ImmediateTrigger at: {}",  DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(LocalDate.now()));
	}

	@Override
	public void start(Consumer<String> trigger) {
      scheduler.initialize();
	  scheduler.schedule(() -> trigger.accept("IMMEDIATE"), new ExactlyOnceTrigger());
	}

	@Override
	public void stop() {
	  if (scheduler != null) {
        scheduler.shutdown();
	  }
	}
	
	private static final class ExactlyOnceTrigger implements Trigger {

	  @Override
	  public Date nextExecutionTime(TriggerContext triggerContext) {		
	    if (triggerContext.lastScheduledExecutionTime() == null) {
	      return new Date();
	    } else {
	      return null;
	    }
	  }	
	}

}

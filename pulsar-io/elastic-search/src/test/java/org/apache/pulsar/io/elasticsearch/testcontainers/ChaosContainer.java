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
package org.apache.pulsar.io.elasticsearch.testcontainers;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Optional;

// see https://github.com/alexei-led/pumba
@Slf4j
public class ChaosContainer<SELF extends ChaosContainer<SELF>> extends GenericContainer<SELF> {

  public static final String PUMBA_IMAGE = Optional.ofNullable(System.getenv("PUMBA_IMAGE"))
          .orElse("gaiaadm/pumba:latest");

  public ChaosContainer(String targetContainer, String pause) {
    super(PUMBA_IMAGE);
    setCommand("--log-level info --interval 60s pause --duration " + pause + " " + targetContainer);
    addFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE);
    setWaitStrategy(Wait.forLogMessage(".*pausing container.*", 1));
    withLogConsumer(o -> {
      log.info("pumba> {}", o.getUtf8String());
    });
  }
}
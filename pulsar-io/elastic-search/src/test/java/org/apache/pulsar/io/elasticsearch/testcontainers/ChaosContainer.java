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
import org.awaitility.Awaitility;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

// see https://github.com/alexei-led/pumba
@Slf4j
public class ChaosContainer<SELF extends ChaosContainer<SELF>> extends GenericContainer<SELF> {

    public static final String PUMBA_IMAGE = Optional.ofNullable(System.getenv("PUMBA_IMAGE"))
            .orElse("gaiaadm/pumba:0.8.0");

    private final List<String> logs = new ArrayList<>();
    private Consumer<ChaosContainer> beforeStop;

    public static ChaosContainer pauseContainerForSeconds(String targetContainer, int seconds) {
        return new ChaosContainer(targetContainer, "pause --duration " + seconds + "s", Wait.forLogMessage(".*pausing container.*", 1),
                (Consumer<ChaosContainer>) chaosContainer -> Awaitility
                        .await()
                        .atMost(seconds + 5, TimeUnit.SECONDS)
                        .until(() -> {
                                    boolean found = chaosContainer.logs.stream().anyMatch((Predicate<String>) line -> line.contains("stop pausing container"));
                                    if (!found) {
                                        log.debug("ChaosContainer stop requested. waiting for \"stop pausing container\" log");
                                        log.debug(String.join("\n", chaosContainer.logs));
                                    }
                                    return found;
                                }
                        ));
    }

    private ChaosContainer(String targetContainer, String command, WaitStrategy waitStrategy, Consumer<ChaosContainer> beforeStop) {
        super(PUMBA_IMAGE);
        setCommand("--log-level info " + command + " " + targetContainer);
        addFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE);
        setWaitStrategy(waitStrategy);
        withLogConsumer(o -> {
            final String string = o.getUtf8String();
            log.info("pumba> {}", string);
            logs.add(string);
        });
        this.beforeStop = beforeStop;
    }

    @Override
    public void stop() {
        if (getContainerId() != null && beforeStop != null) {
            beforeStop.accept(this);
        }
        super.stop();
    }
}
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
package org.apache.pulsar.tests.integration.containers;

import lombok.extern.slf4j.Slf4j;

/**
 * S3 simulation container
 */
@Slf4j
public class S3Container extends ChaosContainer<S3Container> {

    public static final String NAME = "s3";
    private static final String IMAGE_NAME = "apachepulsar/s3mock:latest";
    private final String hostname;

    public S3Container(String clusterName, String hostname) {
        super(clusterName, IMAGE_NAME);
        this.hostname = hostname;
        this.withEnv("initialBuckets", "pulsar-integtest");
    }

    @Override
    public String getContainerName() {
        return clusterName + "-" + hostname;
    }

    @Override
    public void start() {
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withHostName(hostname);
            createContainerCmd.withName(getContainerName());
        });

        super.start();
        log.info("Start s3 service");
    }
}

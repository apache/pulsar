/*
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
package org.apache.pulsar.metadata;

import io.oxia.testcontainers.OxiaContainer;
import lombok.CustomLog;
import org.apache.pulsar.tests.OxiaTestImage;
import org.testcontainers.utility.DockerImageName;

/**
 * Starts the Oxia server of the Oxia-backed tests, from {@link OxiaTestImage#NAME}.
 */
@CustomLog
public final class OxiaTestServer {

    /**
     * Starts an Oxia server with the given number of shards, and logs the Oxia commit it runs, since the image tag
     * moves every day.
     */
    public static OxiaContainer start(int shards) {
        OxiaContainer oxiaServer = new OxiaContainer(DockerImageName.parse(OxiaTestImage.NAME)).withShards(shards);
        oxiaServer.start();
        String revision = oxiaServer.getContainerInfo().getConfig().getLabels().get(OxiaTestImage.REVISION_LABEL);
        log.info()
                .attr("image", OxiaTestImage.NAME)
                .attr("revision", revision)
                .log("Oxia server started");
        return oxiaServer;
    }

    private OxiaTestServer() {
    }
}

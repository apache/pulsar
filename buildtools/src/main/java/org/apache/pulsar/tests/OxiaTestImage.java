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
package org.apache.pulsar.tests;

/**
 * The Oxia Docker image that the Oxia-backed tests run against.
 */
public final class OxiaTestImage {

    /**
     * Oxia's {@code nightly-beta} image, which Oxia's
     * <a href="https://github.com/oxia-db/oxia/blob/main/.github/workflows/ci-nightly-images.yaml">nightly images
     * workflow</a> rebuilds every day from Oxia's development branch. The tests run against it, rather than
     * {@code nightly-stable}, which the workflow builds from the current release branch, so that an Oxia change that
     * breaks Pulsar shows up before it reaches an Oxia release.
     *
     * <p>If an Oxia nightly breaks the Pulsar tests, pin an Oxia release tag here, such as {@code oxia/oxia:0.16.11}.
     *
     * <p>Testcontainers pulls an image only when it is missing, so a local run keeps the nightly it pulled first. Run
     * {@code docker pull oxia/oxia:nightly-beta} before reproducing a CI failure. The tests log the Oxia commit they
     * run against, from {@link #REVISION_LABEL}.
     */
    public static final String NAME = "oxia/oxia:nightly-beta";

    /**
     * The label of the Oxia image that holds the Oxia commit the image was built from.
     */
    public static final String REVISION_LABEL = "org.opencontainers.image.revision";

    private OxiaTestImage() {
    }
}

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
package org.apache.pulsar.common.util.tls;

import java.security.Provider;

/**
 * A {@link Provider} that is discoverable through {@code META-INF/services/java.security.Provider} (see the
 * matching entry in this module's test resources) and registers no services.
 *
 * <p>It models the shape that makes provider resolution order matter: the <em>mode</em> is a constructor
 * argument, so the no-arg instance {@code ServiceLoader} builds differs from an instance an operator
 * constructs and registers themselves under the same name. That is exactly BouncyCastle's JSSE provider,
 * which registers as {@code BCJSSE} whether or not it was built in FIPS mode
 * ({@code new BouncyCastleJsseProvider("fips:BCFIPS")}).
 *
 * <p>The {@code META-INF/services} entry that makes this discoverable is visible to the whole
 * {@code pulsar-common} test source set, deliberately: being reachable through {@code ServiceLoader} is
 * the property under test, so installing it with {@code Security.addProvider} in a {@code @BeforeClass}
 * would exercise the registered-provider branch instead — the opposite of what the test asserts. The
 * blast radius is bounded because this provider registers <b>no services at all</b>: it can never satisfy
 * a {@code getInstance()} call, and is visible only to code enumerating {@code ServiceLoader<Provider>},
 * where it costs one no-op construction.
 */
public final class ServiceLoadableTestProvider extends Provider {

    private static final long serialVersionUID = 1L;

    /** The name both the ServiceLoader-built and the operator-registered instance share. */
    public static final String NAME = "PIP478-RESOLUTION-ORDER";

    /** The {@code info} of the instance {@code ServiceLoader} builds through this no-arg constructor. */
    public static final String SERVICE_LOADER_MODE = "serviceloader-no-arg-instance";

    /** Required by {@link java.util.ServiceLoader}. */
    public ServiceLoadableTestProvider() {
        this(SERVICE_LOADER_MODE);
    }

    public ServiceLoadableTestProvider(String mode) {
        super(NAME, "1.0", mode);
    }
}

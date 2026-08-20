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
package org.apache.pulsar.functions.runtime;

import org.apache.pulsar.client.schema.AvroTrustedClasses;
import org.apache.pulsar.common.classification.InterfaceAudience;

/**
 * Lets a deployed function or connector use {@code Schema.AVRO(...)} over its own classes.
 *
 * <p>Avro 1.12.2 only reflects over classes that are explicitly trusted, and a function's POJOs
 * belong to whoever deployed the function rather than to Pulsar. Since Pulsar loads that code into a
 * class loader it created for the purpose, trusting the class loader trusts exactly the classes that
 * were deployed — nothing else on the JVM's class path.
 *
 * <p>This covers all three runtimes: the process and Kubernetes runtimes reach the same code through
 * {@code JavaInstanceStarter} inside the function's own JVM.
 *
 * <p>Broker types are not this class's concern; the broker declares its own.
 */
@InterfaceAudience.Private
public final class FunctionAvroTrustedClasses {

    private FunctionAvroTrustedClasses() {
    }

    /** Trusts the classes a function's class loader defined, for as long as the instance runs. */
    public static void trustFunctionClassLoader(ClassLoader functionClassLoader) {
        AvroTrustedClasses.trustClassLoader(functionClassLoader);
    }

    /**
     * Stops trusting a function's class loader once its instance has stopped. Classes Avro already
     * resolved stay resolvable, since Avro caches them, so this governs future resolutions.
     */
    public static void untrustFunctionClassLoader(ClassLoader functionClassLoader) {
        AvroTrustedClasses.untrustClassLoader(functionClassLoader);
    }
}

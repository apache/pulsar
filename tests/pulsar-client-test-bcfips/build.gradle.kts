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

plugins {
    id("pulsar.java-conventions")
    id("pulsar.test-certs-conventions")
}

// FIPS is selected by EXCLUDING the non-FIPS BouncyCastle libraries (bcprov/bcpkix/bcutil) that the
// broker and client pull in transitively, leaving only the FIPS provider (bc-fips) on the classpath.
// The non-FIPS and FIPS jars both define org.bouncycastle.* classes with different signers, so having
// both on one classpath triggers CryptoServicesRegistrar signer conflicts.
configurations.all {
    exclude(group = "org.bouncycastle", module = "bcprov-jdk18on")
    exclude(group = "org.bouncycastle", module = "bcprov-ext-jdk18on")
    exclude(group = "org.bouncycastle", module = "bcpkix-jdk18on")
    exclude(group = "org.bouncycastle", module = "bcutil-jdk18on")
}

dependencies {
    implementation(libs.slog)
    testImplementation(libs.bc.fips)
    testImplementation(libs.bcpkix.fips)
    testImplementation(libs.bcutil.fips)
    testImplementation(project(":pulsar-common"))
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testJar"))
    testImplementation(project(":pulsar-client-original"))
    testImplementation(libs.guava)
}

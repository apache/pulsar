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
/**
 * A standalone Pulsar client application that is compiled to a GraalVM native image to
 * verify that the native-image metadata embedded in {@code pulsar-client-original} and
 * {@code pulsar-client-admin-original} is sufficient to build and run a downstream
 * application. Driven by {@code NativeImageSmokeTest} in this module's test sources.
 */
package org.apache.pulsar.tests.integration.nativeimage;

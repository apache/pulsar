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

// Add shared test certificates as a test resource directory.
// These modules rely on the shared tests/certificate-authority directory for TLS test certs.
// By adding it as a source set resource (under "certificate-authority/"), Gradle's
// processTestResources handles the copy and all downstream tasks see it automatically.

// Some modules already have certificate-authority files in their own test resources,
// creating duplicates with the shared directory above.
tasks.named<ProcessResources>("processTestResources") {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
    // Add only the certificate-authority directory from the shared tests/ folder.
    // This is done via from() instead of srcDir() + include() because an include()
    // on the source set would filter ALL test resource directories, not just this one,
    // causing files in src/test/resources (e.g. .htpasswd) to be excluded.
    from(rootProject.file("tests")) {
        include("certificate-authority/**")
    }
}

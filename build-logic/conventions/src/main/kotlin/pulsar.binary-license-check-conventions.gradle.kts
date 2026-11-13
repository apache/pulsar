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

import org.gradle.api.file.RegularFileProperty

// Convention plugin: registers the `checkBinaryLicense` and `updateBinaryLicense` tasks for a
// distribution module.
//
// Consumers wire the produced tarball lazily, e.g.:
//   binaryLicenseCheck { archive.set(serverDistTar.flatMap { it.archiveFile }) }
// The provider chain carries the task dependency on the producing tar task without
// resolving it at configuration time, keeping configuration-cache and
// configure-on-demand happy.
//
// `licenseFile` and `noticeFile` are the source files that the tarball ships as LICENSE and
// NOTICE, which `updateBinaryLicense` rewrites; they default to src/assemble/*.bin.txt.

interface BinaryLicenseCheckExtension {
    val archive: RegularFileProperty
    val licenseFile: RegularFileProperty
    val noticeFile: RegularFileProperty
}

val extension = extensions.create<BinaryLicenseCheckExtension>("binaryLicenseCheck")
extension.licenseFile.convention(layout.projectDirectory.file("src/assemble/LICENSE.bin.txt"))
extension.noticeFile.convention(layout.projectDirectory.file("src/assemble/NOTICE.bin.txt"))

tasks.register<CheckBinaryLicenseTask>("checkBinaryLicense") {
    group = "verification"
    description = "Check LICENSE/NOTICE coverage of bundled jars in the binary distribution tarball"
    binaryDistribution.set(extension.archive)
    report.set(layout.buildDirectory.file("reports/binary-license-check/result.txt"))
}

tasks.register<UpdateBinaryLicenseTask>("updateBinaryLicense") {
    group = "build"
    description = "Update the jar versions in the LICENSE/NOTICE source files to the jars bundled in the binary distribution tarball"
    binaryDistribution.set(extension.archive)
    licenseFile.set(extension.licenseFile)
    noticeFile.set(extension.noticeFile)
}

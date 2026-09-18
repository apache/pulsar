<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Publishing Maven artifacts

Run the commands below from the repository root using `./gradlew`. The shared publication and
repository conventions live in [`conventions/src/main/kotlin`](conventions/src/main/kotlin).

## Ordinary publishing

By default (`publishApiAndSpiOnly` unset or `false`), Gradle publishes the modules that apply the
publishing conventions, including the root `pulsar` parent POM and the full `pulsar-bom`. The
repository's `gradle.properties` supplies the default group (`org.apache.pulsar`) and version.

As in the `branch-4.2` Maven build, publications for `managed-ledger`, `pulsar-broker`,
`pulsar-broker-common`, `pulsar-metadata`, and `pulsar-package-core` include a `tests` classifier
JAR. These test JARs are published in both ordinary and API/SPI mode. In Maven,
`<type>test-jar</type>` and `<classifier>tests</classifier>` refer to the same test JAR; declare
only one dependency per module. Test JARs share their module's POM and do not expose its test
dependencies transitively, so consumers must declare any additional test dependencies they need.

Choose the task for the intended destination:

| Task | Destination | Version requirement |
| --- | --- | --- |
| `publishAllPublicationsToLocalDeployRepository` | `build/local-deploy-repo` | Any version |
| `publishToMavenLocal` | Local Maven repository, normally `~/.m2/repository` | Any version |
| `publishAllPublicationsToApacheSnapshotsRepository` | ASF Nexus snapshots | Ends in `-SNAPSHOT` |
| `publishAllPublicationsToApacheReleasesRepository` | ASF Nexus staging | Does not end in `-SNAPSHOT` |

The remote publishing examples below assume a non-`SNAPSHOT` version. For snapshots, use the
alternative task noted in each example.

```bash
# Inspect the complete publication set in a local directory.
./gradlew publishAllPublicationsToLocalDeployRepository

# Publish a release using credentials supplied by the environment or properties file.
# For snapshots, use publishAllPublicationsToApacheSnapshotsRepository instead.
./gradlew publishAllPublicationsToApacheReleasesRepository
```

Use an unqualified task name from the root to publish across projects. A fully qualified task such
as `:pulsar-client-api:publishMavenPublicationToApacheReleasesRepository` publishes just that
project; it does not publish its dependency artifacts. Avoid the generic `publish` task when you
intend to target only one repository: it targets all configured publishing repositories.

### ASF release staging and signing

For an Apache Pulsar release, follow the canonical
[release process: stage artifacts in ASF Nexus](https://github.com/apache/pulsar-site/blob/main/contribute/release-process.md#stage-artifacts-in-the-asf-nexus-repository)
for release preparation, signing-key setup, staging, and closing the staging repository. From a
prepared release checkout with a non-snapshot version, the Gradle upload command is:

```bash
 # ASF_USERNAME, ASF_PASSWORD and APACHE_USER_GPGID are supplied by your environment.
 # For snapshots, use publishAllPublicationsToApacheSnapshotsRepository
 # and the apacheSnapshotsUsername/apacheSnapshotsPassword properties.
 ORG_GRADLE_PROJECT_apacheReleasesUsername="$ASF_USERNAME" \
ORG_GRADLE_PROJECT_apacheReleasesPassword="$ASF_PASSWORD" \
./gradlew publishAllPublicationsToApacheReleasesRepository \
  -PuseGpgCmd=true -Psigning.gnupg.keyName="$APACHE_USER_GPGID"
```

`-PuseGpgCmd=true` enables command-line GPG signing. `signing.gnupg.keyName` selects a key; if
omitted, GPG uses its default key. Signing tasks are disabled when no signing configuration is
present, which supports local development. Configure signing for ASF release publication.
The build serializes Maven uploads within one Gradle invocation, so `--no-parallel` is unnecessary.
For ASF staging, finish and close one staging repository before starting another release upload.

## Custom repositories, groups and credentials

The existing repository definitions can target a custom Maven repository without editing build
scripts. Override these Gradle properties:

| Repository | URL property | Credential properties |
| --- | --- | --- |
| `apacheSnapshots` | `apacheSnapshotsRepoUrl` | `apacheSnapshotsUsername`, `apacheSnapshotsPassword` |
| `apacheReleases` | `apacheReleasesRepoUrl` | `apacheReleasesUsername`, `apacheReleasesPassword` |

The task names and snapshot/release version checks stay the same even when the URLs are overridden.
For example, a GitHub Packages destination still uses `publishAllPublicationsToApacheSnapshotsRepository`
for snapshots and `publishAllPublicationsToApacheReleasesRepository` for release versions.

Set `group` to publish under a different Maven group. It applies to the parent POM, module
coordinates, BOM entries and publication-only Pulsar dependency replacements. Changing the group
does not change Java package names or imports, so switching to the custom group requires dependency
configuration changes only; no application source-code changes are required because of the group
change. The group override works with either ordinary publishing or the API/SPI subset.
Set a custom build version with
`-Pversion=5.0.0+mypatch.1`; it applies to all published artifacts, including the parent POM and BOM.
Use a version ending in `-SNAPSHOT` when publishing to the snapshots repository.

### Properties file in ephemeral CI

On an ephemeral CI worker, with `REPO_USERNAME` and `REPO_PASSWORD` supplied by CI secrets, the
following configures both repository definitions in the Gradle user properties file. The publishing
commands below set the custom group with `-Pgroup=myorg.pulsar`.
This example assumes the default Gradle user home, `~/.gradle`; when `GRADLE_USER_HOME` is set, use
`$GRADLE_USER_HOME/gradle.properties` instead.

```bash
mkdir -p ~/.gradle
cat >> ~/.gradle/gradle.properties <<EOL
apacheSnapshotsRepoUrl=https://maven.pkg.github.com/myorg/pulsar
apacheSnapshotsUsername=${REPO_USERNAME}
apacheSnapshotsPassword=${REPO_PASSWORD}
apacheReleasesRepoUrl=https://maven.pkg.github.com/myorg/pulsar
apacheReleasesUsername=${REPO_USERNAME}
apacheReleasesPassword=${REPO_PASSWORD}
EOL
```

The properties file contains the expanded credentials. Keep it private to the CI worker, outside
version control, and do not print it in build logs. Values use Java properties syntax; credentials
containing backslashes or line breaks need escaping. Environment variables avoid that encoding step.

Publish a custom release:

```bash
./gradlew publishAllPublicationsToApacheReleasesRepository \
  -Pgroup=myorg.pulsar -Pversion=5.0.0+mypatch.1
```

Publish a custom snapshot:

```bash
./gradlew publishAllPublicationsToApacheSnapshotsRepository \
  -Pgroup=myorg.pulsar -Pversion=5.0.0+mypatch.1-SNAPSHOT
```

### Environment variables and command-line overrides

Prefix a property name with `ORG_GRADLE_PROJECT_` to supply it through the environment:

```bash
 # For snapshots, use publishAllPublicationsToApacheSnapshotsRepository and a -SNAPSHOT version,
 # and replace apacheReleases with apacheSnapshots in the environment variable names.
 ORG_GRADLE_PROJECT_apacheReleasesRepoUrl=https://maven.pkg.github.com/myorg/pulsar \
ORG_GRADLE_PROJECT_apacheReleasesUsername="$REPO_USERNAME" \
ORG_GRADLE_PROJECT_apacheReleasesPassword="$REPO_PASSWORD" \
./gradlew publishAllPublicationsToApacheReleasesRepository \
  -Pgroup=myorg.pulsar -Pversion=5.0.0+mypatch.1
```

The credential command blocks intentionally begin with a space for use in interactive shells.
To omit commands beginning with a space from history, enable `HISTCONTROL=ignorespace` (or
`ignoreboth`) in [Bash](https://www.gnu.org/software/bash/manual/html_node/Bash-Variables.html), or
`setopt HIST_IGNORE_SPACE` in [Zsh](https://zsh.sourceforge.io/Doc/Release/Options.html#History).
Many shell configurations already enable this behavior; check yours before entering credentials.
Always prefix interactive password assignments with a space, including a separate `export` or
assignment used to prepare `REPO_PASSWORD` or `ASF_PASSWORD`. If a block begins with a comment,
keep the space before `#` and also before the first line of the actual command. Continuation lines
following `\` do not need an additional leading space. This history guidance applies to interactive
shells; ordinary non-interactive CI scripts do not record shell history.

All these properties also accept `-Pname=value` on the Gradle command line. Prefer a private
properties file or environment variables for passwords: `-P...Password=...` exposes the value as a
process argument and can record it in shell history. Keep shell tracing disabled when handling
credentials in CI.

Command-line `-P` values override properties files. User-level `gradle.properties` overrides the
repository's properties file, and properties files override `ORG_GRADLE_PROJECT_` variables.
Consequently, `ORG_GRADLE_PROJECT_group` alone does not override this repository's existing `group`
property; use the user properties file or `-Pgroup=myorg.pulsar`. See
[Gradle project properties](https://docs.gradle.org/current/userguide/build_environment.html#sec:project_properties)
for the full precedence rules.

### Using the custom artifacts in a Gradle or Maven build

Configure the custom Maven repository, including any read credentials, in the consuming build's
dependency repositories. The publishing URL properties above configure upload destinations only.
In a Gradle consumer, add this dependency substitution rule to `build.gradle.kts` to replace the
`org.apache.pulsar` group with `myorg.pulsar` for direct and transitive dependencies:

```kotlin
import org.gradle.api.artifacts.component.ModuleComponentSelector

configurations.configureEach {
    val customPulsarVersion = "5.0.0+mypatch.1"
    resolutionStrategy.dependencySubstitution.all {
        val module = requested
        if (module is ModuleComponentSelector && module.group == "org.apache.pulsar") {
            useTarget(
                "myorg.pulsar:${module.module}:$customPulsarVersion",
                "Use the custom Pulsar API/SPI build"
            )
        }
    }
}
```

The rule retains each artifact ID and selects the custom version. Apply it to each consuming
project, for example through a shared convention plugin. Every requested Pulsar artifact must be
available in the custom repository; when publishing only the API/SPI subset, dependencies on
artifacts outside that subset need a narrower substitution rule or additional publications.
See [Gradle dependency substitution](https://docs.gradle.org/current/userguide/resolution_rules.html#sec:dependency_substitution_rules).

With Maven builds, switching to a custom group for the Pulsar API/SPI artifacts is more complicated.
Exclude the original `org.apache.pulsar` artifacts explicitly from each dependency that introduces
them, and add explicit dependencies on the replacement `myorg.pulsar` artifacts and custom version.
Maven dependency management can manage versions, but changing the group creates different artifact
coordinates; it does not redirect dependencies on the original group. Maven exclusions apply to
individual dependency subtrees, so account for every path that brings in the original artifacts.
See [Maven dependency exclusions](https://maven.apache.org/guides/introduction/introduction-to-optional-and-excludes-dependencies.html#dependency-exclusions).

## Publishing the API/SPI subset

API/SPI publication provides the Maven dependencies needed by applications using the Pulsar Java
client, Pulsar Functions, and Pulsar plugins such as interceptors and additional servlets. The set
includes the client implementations (including v5), API and service provider interface (SPI)
libraries, and their transitive Pulsar dependencies.

The selection also includes the `buildtools` JAR and all five test JAR publications described
above to support external tests.

A typical use case is distributing a custom Pulsar Java client build to your own Maven repository
so applications can quickly adopt a bug fix or security fix in Pulsar itself. Publishing this subset
avoids performing a full Pulsar dependency release to that repository. For deploying a custom server
build, the Docker build is sufficient; API/SPI publication supplies the artifacts consumed by
applications, Functions and plugins outside that server image.

> **Note:** This custom-build use case is not intended for handling CVEs in transitive third-party
> client dependencies. With Pulsar 5, the recommended approach for dependencies bundled inside
> shaded JARs is to switch to **unshaded client dependencies** and update the affected dependency
> through the consuming build's dependency management. There is no need to create a custom Pulsar
> build just to replace a transitive dependency when using unshaded JARs. See the
> [`pulsar-client-v5-all` guide](../pulsar-client-v5-all/README.md) for the unshaded aggregate and
> migration instructions, including the exclusions needed to remove old shaded artifacts. This
> also applies to applications using the ordinary v4 client API: the aggregate includes v4, v5
> and admin implementations, so switching dependencies does not require migrating source code to
> the v5 API. Verify compatibility with the replacement dependency.

Use `-PpublishApiAndSpiOnly=true` to select this publication mode. The explicit project selection lives
in [`PulsarApiSpiPublication.projects`](conventions/src/main/kotlin/PulsarApiSpiPublication.kt).
Add a project's Gradle path there to include it. Without the property, publishing uses the normal
release publication set.

Override the group with `-Pgroup=<your.group>`. The parent POM and publication-only dependency
replacements use the same group. In API/SPI mode, `pulsar-bom` retains only constraints for selected
projects, in both its Maven POM and Gradle Module Metadata.

```bash
# Validate the selected artifacts' generated POMs and Gradle Module Metadata, without uploading.
./gradlew validateApiSpiPublication -PpublishApiAndSpiOnly=true -Pgroup=com.example.pulsar

# Publish the complete selection to build/local-deploy-repo for inspection.
./gradlew publishAllPublicationsToLocalDeployRepository \
  -PpublishApiAndSpiOnly=true -Pgroup=com.example.pulsar
```

Use the unqualified `publishAllPublicationsTo<Repository>Repository` task from the repository root
for any configured Maven repository. Gradle selects that task across projects; projects outside the
selection have no publications in this mode. A fully qualified per-project task publishes only that
project, so use the unqualified command when publishing the complete set to a fresh repository.

To publish the subset to a custom releases repository configured as above:

```bash
# For snapshots, use publishAllPublicationsToApacheSnapshotsRepository and a -SNAPSHOT version.
./gradlew publishAllPublicationsToApacheReleasesRepository \
  -PpublishApiAndSpiOnly=true -Pgroup=myorg.pulsar -Pversion=5.0.0+mypatch.1
```

Every Maven upload (including `publishToMavenLocal`) in this mode depends on
`validateApiSpiPublication`. Validation checks the generated metadata of **every selected project**,
including parent references, platform constraints and publication-only dependency replacements.
Every Pulsar coordinate must identify another selected publication at the same published version;
checking all such edges establishes transitive closure. Missing projects fail validation before any
upload, with the referring artifact and missing coordinate. Test/build dependencies and dependencies
bundled inside shaded artifacts do not need separate publication unless the generated metadata
references them. Generating Gradle metadata can build JARs, so this is not a source-only check.

Both configure-on-demand and configuration cache remain supported. A scoped build such as
`:pulsar-client-api:assemble` configures only the projects it needs even with API/SPI mode enabled.
Validation and publication intentionally configure the entire selected publication set and the
build dependencies needed to generate its metadata.

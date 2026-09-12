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

# Combined Java client and admin dependencies

Use one combined dependency for applications using the Pulsar Java client,
the admin API, or both. **Both choices include the v4 client, v5 client, and admin
implementation.** Applications using the v4 API can migrate their dependency before
migrating source code to the v5 API.

| Artifact | Dependency graph |
| --- | --- |
| `pulsar-client-v5-all` | Unshaded aggregate; resolves the client/admin implementations transitively |
| `pulsar-client-v5-shaded` | One jar containing relocated client/admin implementations and third-party dependencies |

The shaded artifact keeps `pulsar-client-api`, `pulsar-client-api-v5`,
`pulsar-client-admin-api`, `pulsar-tls-factory-api`, and `pulsar-http-client-api`
unshaded as external dependencies. Logging, BouncyCastle, and other intentionally
non-bundled libraries remain external too. Applications using protobuf schemas must
also provide `protobuf-java`.

## Unshaded dependency

Gradle Kotlin DSL (`pulsarVersion` is your Pulsar release version):

```kotlin
dependencies {
    implementation("org.apache.pulsar:pulsar-client-v5-all:$pulsarVersion")
}
```

Maven (`pulsar.version` is your Pulsar release version):

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-client-v5-all</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

## Shaded dependency

Use `pulsar-client-v5-shaded` as an ordinary dependency, with no classifier,
variant attributes, or implementation exclusions required on this dependency.
Despite its name, it includes the admin implementation and v4 client too.

### Gradle

```kotlin
dependencies {
    implementation("org.apache.pulsar:pulsar-client-v5-shaded:$pulsarVersion")
}
```

### Maven

Add this to the `<dependencies>` section of your `pom.xml`:

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-client-v5-shaded</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Why a separate artifact instead of a classifier?

[Maven classifiers](https://maven.apache.org/pom.html) share the main artifact's
POM and dependency list. Selecting a
shaded classifier on an unshaded aggregate still pulls in the unshaded client/admin
implementations and their transitive dependencies. The classifier changes the jar
selected, not its dependency graph.

`pulsar-client-v5-shaded` has its own dependency-reduced POM, containing only the
external APIs and intentionally non-bundled libraries. Both Maven and Gradle can
therefore select it with one ordinary dependency declaration. No shaded classifier
is published for `pulsar-client-v5-all`.

## Migrating existing applications

Migrate applications, including those still using the v4 client API, to either
`pulsar-client-v5-all` or `pulsar-client-v5-shaded`. Replace separate `pulsar-client`
and `pulsar-client-admin` dependencies, and replace the older `pulsar-client-all`
aggregate where present. The admin Java artifact is named `pulsar-client-admin`;
`pulsar-admin` is the CLI name.

Also exclude `pulsar-client` and `pulsar-client-admin` from dependencies that pull
them in transitively. Keeping those shaded jars alongside the combined dependency
duplicates client/admin implementations and bundled libraries on the classpath.
For the shaded choice, also remove separately declared unshaded implementations.

For example, apply these exclusions to Gradle application configurations:

```kotlin
configurations.configureEach {
    exclude(group = "org.apache.pulsar", module = "pulsar-client")
    exclude(group = "org.apache.pulsar", module = "pulsar-client-admin")
}
```

In Maven, add equivalent exclusions to **each dependency** that introduces the
legacy artifacts; Maven exclusions apply to a dependency's subtree, not globally:

```xml
<exclusions>
  <exclusion>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-client</artifactId>
  </exclusion>
  <exclusion>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-client-admin</artifactId>
  </exclusion>
</exclusions>
```

Inspect the resolved runtime dependency graph to verify that the legacy artifacts
are gone. Code that directly imports relocated implementation or third-party
classes must migrate to the public APIs or use the unshaded aggregate.

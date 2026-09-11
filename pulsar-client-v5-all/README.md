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

# Combined Java client and admin dependency

Use `org.apache.pulsar:pulsar-client-v5-all` for applications using the Pulsar Java
client, the admin API, or both. It includes the **v4 and v5 client implementations**
as well as the admin implementation. Applications using the v4 API can migrate
this dependency before migrating their source code to the v5 API.

Choose one of these forms:

| Form | Contents | Dependency selection |
| --- | --- | --- |
| Ordinary artifact | Unshaded aggregate depending on the v5 client and unshaded admin implementation | No classifier |
| Shaded artifact | One jar bundling the v4/v5 clients, admin implementation, and relocated third-party dependencies | Gradle shaded variant; Maven `all` classifier |

Both forms keep `pulsar-client-api`, `pulsar-client-api-v5`,
`pulsar-client-admin-api`, `pulsar-tls-factory-api`, and `pulsar-http-client-api`
unshaded as external dependencies. Logging, BouncyCastle, and the other
non-bundled dependencies remain external too. Applications using protobuf schemas
must also provide `protobuf-java`.

## Ordinary dependency

The default artifact resolves the unshaded implementations transitively. Examples
and applications need only the aggregate dependency, without naming the individual
implementation artifacts.

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

### Gradle

Gradle consumers should select the shaded variant. Gradle Module Metadata selects
both the `all` jar and the matching external dependencies, without pulling in the
unshaded implementation graph. Prefer this to selecting a classifier directly.

```kotlin
import org.gradle.api.attributes.Bundling

dependencies {
    implementation("org.apache.pulsar:pulsar-client-v5-all:$pulsarVersion") {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.SHADOWED))
        }
    }
}
```

### Maven

Add the following dependency to the `<dependencies>` section of your `pom.xml`,
with `pulsar.version` set to your Pulsar release version. The `all` classifier
selects the shaded jar.

Maven classifiers share one POM. Consequently, selecting `all` alone still brings
in the ordinary artifact's unshaded implementation dependencies. Exclude the three
bundled implementation roots below; the API modules and non-bundled dependencies
remain available through direct dependencies in the shared POM.

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-client-v5-all</artifactId>
  <version>${pulsar.version}</version>
  <classifier>all</classifier>
  <exclusions>
    <exclusion>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>pulsar-client-v5</artifactId>
    </exclusion>
    <exclusion>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>pulsar-client-admin-original</artifactId>
    </exclusion>
    <exclusion>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>pulsar-client-messagecrypto-bc</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

## Migrating existing applications

Migrate applications, including those still using the v4 client API, to
`pulsar-client-v5-all` in its ordinary or shaded form. Replace separate
`pulsar-client` and `pulsar-client-admin` dependencies, and replace the older
`pulsar-client-all` aggregate where present. The admin Java artifact is named
`pulsar-client-admin`; `pulsar-admin` is the CLI name.

Also exclude `pulsar-client` and `pulsar-client-admin` from dependencies that pull
them in transitively. Keeping those shaded jars alongside the aggregate duplicates
client/admin implementations and bundled libraries on the classpath. For the
shaded choice, also remove any separately declared unshaded implementations.

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
classes must migrate to the public APIs or use the ordinary unshaded form.

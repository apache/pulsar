---
id: version-2.5.2-security-bouncy-castle
title: Bouncy Castle Providers
sidebar_label: Bouncy Castle Providers
original_id: security-bouncy-castle
---

## BouncyCastle Introduce

`Bouncy Castle` is a Java library that complements the default Java Cryptographic Extension (JCE), 
and it many more cipher suites and algorithms than the default JCE provided by Sun.

In addition to that, `Bouncy Castle` has lots of utilities for reading arcane formats like PEM and ASN.1 that no sane person would want to rewrite themselves.

In Pulsar, security and crypto have dependencies on BouncyCastle Jars. For the detailed installing and configuring Bouncy Castle FIPS, see [BC FIPS Documentation](https://www.bouncycastle.org/documentation.html), especially the **User Guides** and **Security Policy** PDFs.

`Bouncy Castle` provides both [FIPS](https://www.bouncycastle.org/fips_faq.html) and non-FIPS version. But in a JVM, you can not include both of the 2 versions, and you need to exclude the current version before include the other.

In Pulsar, the security and crypto methods also depends on `Bouncy Castle`, especially in [TLS Authentication](security-tls-authentication.md) and [Transport Encryption](security-encryption.md). This document contains the configuration between BouncyCastle FIPS(BC-FIPS) and non-FIPS(BC-non-FIPS) version while using Pulsar.

## Include dependencies of BC-non-FIPS

By default, BouncyCastle non-FIPS version is build along with Pulsar's Broker and Java client.

Pulsar module `bouncy-castle-bc`, which defined by `bouncy-castle/bc/pom.xml` contains the needed non-FIPS jars for Pulsar.

```xml
    <dependency>
      <groupId>org.bouncycastle</groupId>
      <artifactId>bcpkix-jdk15on</artifactId>
      <version>${bouncycastle.version}</version>
    </dependency>

    <dependency>
      <groupId>org.bouncycastle</groupId>
      <artifactId>bcprov-ext-jdk15on</artifactId>
      <version>${bouncycastle.version}</version>
    </dependency>
```

By using this `bouncy-castle-bc` module, user can easily include and exclude BouncyCastle non-FIPS jars.

### Pulsar Client and Broker dependencies on BC-non-FIPS

Pulsar Client(`pulsar-client-original`) module include BouncyCastle non-FIPS jars by add dependency like this:

```xml
    <dependency>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>bouncy-castle-bc</artifactId>
      <version>${project.parent.version}</version>
      <classifier>pkg</classifier>
    </dependency>
```

And Pulsar Broker (`pulsar-broker`) module include BouncyCastle non-FIPS jars by indirectly include Pulsar Client(`pulsar-client-original`) module.
```xml
    <dependency>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>pulsar-client-original</artifactId>
      <version>${project.version}</version>
    </dependency>
```

## Exclude BC-non-FIPS and include BC-FIPS

After understanding the above dependencies, user can easily exclude non-FIPS version and include FIPS version.

### BC-FIPS

Pulsar module `bouncy-castle-bcfips`, which defined by `bouncy-castle/bcfips/pom.xml` contains the needed FIPS jars for Pulsar.

```xml
    <dependency>
      <groupId>org.bouncycastle</groupId>
      <artifactId>bc-fips</artifactId>
      <version>${bouncycastlefips.version}</version>
    </dependency>

    <dependency>
      <groupId>org.bouncycastle</groupId>
      <artifactId>bcpkix-fips</artifactId>
      <version>${bouncycastlefips.version}</version>
    </dependency>
```

User can choose include module `bouncy-castle-bcfips` module directly, or include original BC-FIPS jars. 

For example:

```xml
    <dependency>
      <groupId>${project.groupId}</groupId>
      <artifactId>pulsar-broker</artifactId>
      <version>${project.version}</version>
      <exclusions>
        <exclusion>
          <groupId>${project.groupId}</groupId>
          <artifactId>bouncy-castle-bc</artifactId>
        </exclusion>
      </exclusions>
    </dependency>

    <!--exclude bouncy castle non-FIPS version, then load fips version-->
    <dependency>
      <groupId>org.bouncycastle</groupId>
      <artifactId>bc-fips</artifactId>
      <version>${bouncycastlefips.version}</version>
    </dependency>

    <dependency>
      <groupId>org.bouncycastle</groupId>
      <artifactId>bcpkix-fips</artifactId>
      <version>${bouncycastlefips.version}</version>
    </dependency>
``` 
 
Besides this, module `bouncy-castle-bcfips` builds contain an output with format NAR, you can set java environment by `-DBcPath='nar/file/path'`, Pulsar will auto load it.

For more example, you can reference module `bcfips-include-test` and `bcfips-nar-test`.



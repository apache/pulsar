---
id: security-bouncy-castle
title: Bouncy Castle Providers
sidebar_label: "Bouncy Castle Providers"
---

## BouncyCastle Introduce

`Bouncy Castle` is a Java library that complements the default Java Cryptographic Extension (JCE), 
and it provides more cipher suites and algorithms than the default JCE provided by Sun.

In addition to that, `Bouncy Castle` has lots of utilities for reading arcane formats like PEM and ASN.1 that no sane person would want to rewrite themselves.

In Pulsar, security and crypto have dependencies on BouncyCastle Jars. For the detailed installing and configuring Bouncy Castle FIPS, see [BC FIPS Documentation](https://www.bouncycastle.org/documentation.html), especially the **User Guides** and **Security Policy** PDFs.

`Bouncy Castle` provides both [FIPS](https://www.bouncycastle.org/fips_faq.html) and non-FIPS version. But in a JVM, you can not include both of the 2 versions, and you need to exclude the current version before include the other.

In Pulsar, the security and crypto methods also depends on `Bouncy Castle`, especially in [TLS Authentication](security-tls-authentication.md) and [Transport Encryption](security-encryption). This document contains the configuration between BouncyCastle FIPS(BC-FIPS) and non-FIPS(BC-non-FIPS) version while using Pulsar.

## How BouncyCastle modules packaged in Pulsar

In Pulsar's `bouncy-castle` module, We provide 2 sub modules: `bouncy-castle-bc`(for non-FIPS version) and `bouncy-castle-bcfips`(for FIPS version), to package BC jars together to make the include and exclude of `Bouncy Castle` easier.

To achieve this goal, we will need to package several `bouncy-castle` jars together into `bouncy-castle-bc` or `bouncy-castle-bcfips` jar.
Each of the original bouncy-castle jar is related with security, so BouncyCastle dutifully supplies signed of each JAR.
But when we do the re-package, Maven shade explodes the BouncyCastle jar file which puts the signatures into META-INF,
these signatures aren't valid for this new, uber-jar (signatures are only for the original BC jar). 
Usually, You will meet error like `java.lang.SecurityException: Invalid signature file digest for Manifest main attributes`.

You could exclude these signatures in mvn pom file to avoid above error, by

```access transformers

<exclude>META-INF/*.SF</exclude>
<exclude>META-INF/*.DSA</exclude>
<exclude>META-INF/*.RSA</exclude>

```

But it can also lead to new, cryptic errors, e.g. `java.security.NoSuchAlgorithmException: PBEWithSHA256And256BitAES-CBC-BC SecretKeyFactory not available`
By explicitly specifying where to find the algorithm like this: `SecretKeyFactory.getInstance("PBEWithSHA256And256BitAES-CBC-BC","BC")`
It will get the real error: `java.security.NoSuchProviderException: JCE cannot authenticate the provider BC`

So, we used a [executable packer plugin](https://github.com/nthuemmel/executable-packer-maven-plugin) that uses a jar-in-jar approach to preserve the BouncyCastle signature in a single, executable jar.

### Include dependencies of BC-non-FIPS

Pulsar module `bouncy-castle-bc`, which defined by `bouncy-castle/bc/pom.xml` contains the needed non-FIPS jars for Pulsar, and packaged as a jar-in-jar(need to provide `<classifier>pkg</classifier>`).

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

By using this `bouncy-castle-bc` module, you can easily include and exclude BouncyCastle non-FIPS jars.

### Modules that include BC-non-FIPS module (`bouncy-castle-bc`)

For Pulsar client, user need the bouncy-castle module, so `pulsar-client-original` will include the `bouncy-castle-bc` module, and have `<classifier>pkg</classifier>` set to reference the `jar-in-jar` package.
It is included as following example:

```xml

  <dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>bouncy-castle-bc</artifactId>
    <version>${pulsar.version}</version>
    <classifier>pkg</classifier>
  </dependency>

```

By default `bouncy-castle-bc` already included in `pulsar-client-original`, And `pulsar-client-original` has been included in a lot of other modules like `pulsar-client-admin`, `pulsar-broker`.  
But for the above shaded jar and signatures reason, we should not package Pulsar's `bouncy-castle` module into `pulsar-client-all` other shaded modules directly, such as `pulsar-client-shaded`, `pulsar-client-admin-shaded` and `pulsar-broker-shaded`. 
So in the shaded modules, we will exclude the `bouncy-castle` modules.

```xml

  <filters>
    <filter>
      <artifact>org.apache.pulsar:pulsar-client-original</artifact>
      <includes>
        <include>**</include>
      </includes>
      <excludes>
        <exclude>org/bouncycastle/**</exclude>
      </excludes>
    </filter>
  </filters>

```

That means, `bouncy-castle` related jars are not shaded in these fat jars.

### Module BC-FIPS (`bouncy-castle-bcfips`)

Pulsar module `bouncy-castle-bcfips`, which defined by `bouncy-castle/bcfips/pom.xml` contains the needed FIPS jars for Pulsar. 
Similar to `bouncy-castle-bc`, `bouncy-castle-bcfips` also packaged as a `jar-in-jar` package for easy include/exclude.

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

### Exclude BC-non-FIPS and include BC-FIPS 

If you want to switch from BC-non-FIPS to BC-FIPS version, Here is an example for `pulsar-broker` module: 

```xml

  <dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-broker</artifactId>
    <version>${pulsar.version}</version>
    <exclusions>
      <exclusion>
        <groupId>org.apache.pulsar</groupId>
        <artifactId>bouncy-castle-bc</artifactId>
      </exclusion>
    </exclusions>
  </dependency>
  
  <dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>bouncy-castle-bcfips</artifactId>
    <version>${pulsar.version}</version>
    <classifier>pkg</classifier>
  </dependency>

```

 
For more example, you can reference module `bcfips-include-test`.


#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
JDK_COMMON_PKGS=java.lang:java.util:java.util.concurrent:java.nio:java.net:java.io

(
  cd $ROOT_DIR

  # Java client
  javadoc \
    -quiet \
    -windowtitle "Pulsar Client Java API" \
    -doctitle "Pulsar Client Java API" \
    -overview site/scripts/javadoc-client.html \
    -d site/api/client \
    -subpackages org.apache.pulsar.client.api \
    -noqualifier $JDK_COMMON_PKGS \
    -notimestamp \
    `find pulsar-client/src/main/java/org/apache/pulsar/client/api -name *.java`

  # Java admin
  javadoc \
    -quiet \
    -windowtitle "Pulsar Admin Java API" \
    -doctitle "Pulsar Admin Java API" \
    -overview site/scripts/javadoc-admin.html \
    -d site/api/admin \
    -noqualifier $JDK_COMMON_PKGS \
    -notimestamp \
    `find pulsar-client-admin -name *.java | grep -v /internal/` \
    `find pulsar-common/src/main/java/org/apache/pulsar/common/policies -name *.java`

  # Broker
  #javadoc \
  #  -quiet \
  #  -windowtitle "Pulsar Broker Java API" \
  #  -doctitle "Pulsar Broker Java API" \
  #  -overview site/scripts/javadoc-broker.html \
  #  -d site/api/broker \
  #  -noqualifier $JDK_COMMON_PKGS \
  #  -notimestamp \
  #  `find pulsar-broker -name *.java`
) || true

# The "|| true" is present here to keep this script from failing due to
# Javadoc errors

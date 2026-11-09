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

# Alpine image with kinesis_producer compiled for Alpine Linux / musl

This directory includes the Docker scripts to build an image with `kinesis_producer` for Alpine Linux.
`kinesis_producer` is a native executable that is required by [Amazon Kinesis Producer library (KPL)](https://github.com/awslabs/amazon-kinesis-producer) which is used by the Pulsar IO Kinesis Sink connector. The default `kinesis_producer` binary is compiled for glibc, and it does not work on Alpine Linux which uses musl.

This image only needs to be re-created when we want to upgrade to a newer version of `kinesis_producer`.

The current version is **1.0.6**, published as `quay.io/datastax/pulsar-io-kinesis-sink-kinesis_producer:1.0.6`.
The previous version, 1.0.4, is available as `apachepulsar/pulsar-io-kinesis-sink-kinesis_producer:1.0.4` on Docker Hub.

# Building locally (for testing)

To build the image locally for your current platform only:
```bash
cd docker/kinesis-producer-alpine
docker build -t kinesis-producer-alpine-test:1.0.6 .
```

# Steps to publish to Quay

1. Change `KINESIS_PRODUCER_LIB_VERSION` in the Dockerfile if upgrading.
2. Update `AWS_SDK_CPP_VERSION` and the `BUILD_ONLY` components in `build-alpine.sh` to match the new KPL release's `bootstrap.sh`.
   Check that `kinesis_producer_alpine.patch` still applies to the new KPL tag.
3. Run the **Build KPL Alpine image** GitHub Actions workflow (`.github/workflows/build-kpl-alpine-image.yaml`)
   with `push` enabled. It builds `linux/amd64` and `linux/arm64` on native runners, smoke tests them, and publishes a multi-arch
   manifest to `quay.io/datastax/pulsar-io-kinesis-sink-kinesis_producer` as `<version>` and `<version>-<date>`.
   The workflow needs the `QUAY_USERNAME` and `QUAY_PASSWORD` repository secrets (a Quay robot account with write access).

The image tag is then used in `docker/pulsar-all/Dockerfile` via the `PULSAR_IO_KINESIS_KPL_IMAGE` build argument.
The `kinesis_producer` binary is copied from the image to the `pulsar-all` image, and `PULSAR_IO_KINESIS_KPL_PATH`
is set so the Kinesis Sink connector knows where to find it.

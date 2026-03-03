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

# Steps

1. Change the version in the Dockerfile for this directory.
2. Rebuild the image and push it to Docker Hub:
```
IMAGE=apachepulsar/pulsar-io-kinesis-sink-kinesis_producer
KINESIS_PRODUCER_VERSION=1.0.4
docker buildx build --platform=linux/amd64,linux/arm64 \
 -t "$IMAGE:$KINESIS_PRODUCER_VERSION" -t "$IMAGE:${KINESIS_PRODUCER_VERSION}-$(date -I)" \
 . --push
```

The image tag is then used in `docker/pulsar-all/Dockerfile`. The `kinesis_producer` binary is copied from the image to the `pulsar-all` image that is used by Pulsar Functions to run the Pulsar IO Kinesis Sink connector. The environment variable `PULSAR_IO_KINESIS_KPL_PATH` is set to `/opt/amazon-kinesis-producer/bin/kinesis_producer` and this is how the Kinesis Sink connector knows where to find the `kinesis_producer` binary.
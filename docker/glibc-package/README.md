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

# GLibc compatibility package

This directory includes the Docker scripts to build an image with GLibc compiled for Alpine Linux. 

This is used to ensure plugins that are going to be used in the Pulsar image and that are depeding on GLibc, will
still be working correctly in the Alpine Image. (eg: Netty Tc-Native and Kinesis Producer Library).

This image only needs to be re-created when we want to upgrade to a newer version of GLibc.

# Steps

1. Change the version in the Dockerfile for this directory.
2. Rebuild the image and push it to Docker Hub:
```
docker buildx build --platform=linux/amd64,linux/arm64 -t apachepulsar/glibc-base:2.38 . --push
```

The image tag is then used in `docker/pulsar/Dockerfile`.

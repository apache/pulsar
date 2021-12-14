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

This folder contains a Docker image that can used to compile the Pulsar C++ client library
and website in a reproducible environment.

```shell
docker build -t pulsar-build .
```

The image is already available at https://hub.docker.com/r/apachepulsar/pulsar-build

Example: `apachepulsar/pulsar-build:ubuntu-16.04`

## Build and Publish pulsar-build image

> Only committers have permissions on publishing pulsar images to `apachepulsar` docker hub.

### Build pulsar-build image

We provide Docker images for multiple architectures including amd64 and arm64, by leveraging Docker build kit, please
make sure to start a BuildKit daemon with command `docker buildx create --use --driver docker-container`.

If you want to build Docker image only for local testing in your current machine (without pushing to a registry), you
can build the image without BuildKit, like this:

```shell
docker build -t apachepulsar/pulsar-build:ubuntu-16.04 .
```

It's an unusual case when you want to build multi-arch images locally without pushing to a registry, but in case you
want to do this, run command:

```shell
docker buildx build --no-cache --platform=linux/amd64,linux/arm64 --load -t apachepulsar/pulsar-build:ubuntu-16.04 .
```

This command builds multi-arch images in your local machine, but be careful when you
run `docker push apachepulsar/pulsar-build:ubuntu-16.04`, **only one** image matching your system architecture will be
pushed.

If you want to push all images with different architectures to remote Docker registry, you can build and push with
command:

```shell
docker buildx build --no-cache --platform=linux/amd64,linux/arm64 --push -t apachepulsar/pulsar-build:ubuntu-16.04 .
```

> Make sure to log into the registry with command `docker login <registry.address>` before running the command above.

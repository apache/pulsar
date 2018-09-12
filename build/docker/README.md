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


```shell
docker build -t apachepulsar/pulsar-build:ubuntu-16.04 .
```

### Publish pulsar-build image

```shell
publish.sh
```

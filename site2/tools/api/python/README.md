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

# Pulsar Client Generated Docs

Pulsar's Python docs used to be generated for every wesbite build, but this is unnecessary. We now generate
the docs for each version by running the [build-docs-in-docker.sh](./build-docs-in-docker.sh) script in this
directory.

## Example

When starting in the root directory of the project, you can run:

```shell
PULSAR_VERSION=2.9.2 ./site2/tools/api/python/build-docs-in-docker.sh
```
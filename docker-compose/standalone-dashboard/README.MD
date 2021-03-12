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
# Docker Compose

In this folder path run the following command to start the cluster

```
docker-compose up
```

## Urls

Assuming localhost the the following endpoint will be available


[web-dashboard](http://localhost:80) pulsar admin dashboard, shows various metrics and metatdata information about the cluster

[broker-admin](http://localhost:8080) access broker rest interface

[broker-service-url](pulsar://locahost:6650)  broker service url for use with producers and consumers


You can shut it down with the following command in folder path

```
docker-compose down
```
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
# Apache Pulsar Docker Compose

In this folder path, run the following command to start the cluster.

```
docker-compose up
```

This command starts individual containers for the following services:

- ZooKeeper (3)
- BookKeeper (3)
- Pulsar (3)
- Proxy (1)
- WebSocket (1)
- Function (1)
- Pulsar Manager (1)
- SQL (1)

## Motivation

Most of what is available with Docker and Apache Pulsar uses the standalone version of Apache Pulsar, which is fantastic. But, users need to take care of all of the complexity of Apache Pulsar.

This aims to create a Docker implementation that allows each individual part to be exposed to learn how it all fits together.

## URLs

Assume that localhost addresses for the following endpoints are available:

- [web-dashboard - http://localhost:9527](http://localhost:9527): pulsar-admin dashboard, showing various metrics and metadata information about the cluster

- [broker-admin - http://localhost:8080](http://localhost:8080): access the broker REST interface

- [broker-service-url - pulsar//:locahost:6650](pulsar//:locahost:6650): broker service URL for use with producers and consumers

You can shut it down with the following command in the folder path:

```
docker-compose down
```

You may need to delete the `./data` folder created by the Pulsar Manager.

## Notes

Docker is notorious for being difficult to manage the startup order of containers. In Pulsar, startup order is very significant. This may cause some containers to fail and have to be restarted.

Generally speaking, everything should be started up successfully in about 2 minutes.

If you plan to use this in production, more care should be taken on restart policies, logging, configuration, etc.

## To-Do

- Add more SQL workers.
- Dig into why PF_configurationStoreServers in fnc1 does not support a list of ZooKeeper instances.
- Figure out why the dashboard ( v0.2.0) does not work correctly.

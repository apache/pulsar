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

To start the cluster, run the following command in the current folder: 

```
docker-compose up
```

## Available endpoints

The following endpoints are available (assuming Docker Compose exposes ports on `localhost`):

* [web-dashboard](http://localhost:80) - Pulsar dashboard, shows various metrics and metadata information about the cluster
* `http://localhost:8080` - broker HTTP REST API URL, for both administrative tasks and for topic lookup by producers and consumers
* `pulsar://locahost:6650` - broker binary protocol URL, for all data transferred by producers and consumers

To shut down the cluster, run the following command in the current folder:

```
docker-compose down
```

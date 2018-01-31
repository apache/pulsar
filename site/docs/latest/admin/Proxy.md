---
title: The Pulsar proxy
---

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

[`proxy`](../../reference/CliTools#pulsar-proxy) command

To start the proxy:

```bash
$ bin/pulsar proxy \
  --zookeeper-servers zk-0,zk-1,zk-2 \
  --global-zookeeper-servers zk-0,zk-1,zk-2
```

## Notes

* You can run multiple proxy nodes
* Stateless proxy over multiple brokers (all brokers in a cluster)
* Helpful when direct connectivity between clients and brokers is either not possible or not desirable (good way to avoid administrative overhead)
* Example: Kubernetes (clients can't access individual brokers and thus require a proxy to connect)
* All lookup and data connections flow through a proxy instance; those instances can be exposed via, for example, a load balancer
* Minimal interaction between client and broker
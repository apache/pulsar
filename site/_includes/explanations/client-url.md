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

To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](../../project/BinaryProtocol) URL.

Pulsar protocol URLs are assigned to specific {% popover clusters %}, use the `pulsar` scheme and have a default port of 6650. Here's an example for `localhost`:

```
pulsar://localhost:6650
```

A URL for a production Pulsar cluster may look something like this:

```
pulsar://pulsar.us-west.example.com:6650
```

If you're using [TLS](../../admin/Authz#tls-client-auth) authentication, the URL will look like something like this:

```
pulsar+ssl://pulsar.us-west.example.com:6651
```

### Global vs. cluster-specific topics

Pulsar {% popover topics %} can be either cluster specific or global. Cluster-specific topic URLs have this structure:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

If the topic that a client needs to publish to or consume from is specific to a cluster, the client will need to use the broker service URL that you assigned to that cluster when [initializing its metadata](../../deployment/InstanceSetup#cluster-metadata-initialization).

If the topic is global, however, the URL for the topic will look like this:

{% include topic.html p="property" c="global" n="namespace" t="topic" %}

In that case, your client can use the broker service URL for *any* cluster in the {% popover instance %} and Pulsar's internal service discovery system will handle the rest.

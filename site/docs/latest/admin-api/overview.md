---
title: The Pulsar admin interface
tags: [admin, cli, rest, java]
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

The Pulsar admin interface enables you to manage all of the important entities in a Pulsar {% popover instance %}, such as {% popover properties %}, {% popover topics %}, and {% popover namespaces %}.

You can currently interact with the admin interface via:

1. Making HTTP calls against the admin [REST API](../../reference/RestApi) provided by Pulsar {% popover brokers %}. For some restful apis, they might be redirected to topic owner brokers for serving
   with [`307 Temporary Redirect`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/307), hence the HTTP callers should handle `307 Temporary Redirect`. If you are using `curl`, you should specify `-L`
   to handle redirections.
1. The `pulsar-admin` CLI tool, which is available in the `bin` folder of your [Pulsar installation](../../getting-started/LocalCluster):

    ```shell
    $ bin/pulsar-admin
    ```

    Full documentation for this tool can be found in the [Pulsar command-line tools](../../reference/CliTools#pulsar-admin) doc.

1. A Java client interface.

{% include message.html id="admin_rest_api" %}

In this document, examples from each of the three available interfaces will be shown.

## Admin setup

{% include explanations/admin-setup.md %}

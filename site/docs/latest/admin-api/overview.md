---
title: The Pulsar admin interface
tags: [admin, cli, rest, java]
---

The Pulsar admin interface enables you to manage all of the important entities in a Pulsar {% popover instance %}, such as {% popover properties %}, {% popover topics %}, and {% popover namespaces %}.

You can currently interact with the admin interface via:

1. Making HTTP calls against the admin [REST API](../../reference/RestApi) provided by Pulsar {% popover brokers %}.
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

---
id: functions-develop-admin-api
title: Call Pulsar admin APIs
sidebar_label: "Call Pulsar admin APIs"
---

Pulsar Functions that use the Java SDK have access to the Pulsar admin client, which allows the Pulsar admin client to manage API calls to your Pulsar clusters.

Below is an example of how to use the Pulsar admin client exposed from the function `context`.

```java

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * In this particular example, for every input message,
 * the function resets the cursor of the current function's subscription to a
 * specified timestamp.
 */
public class CursorManagementFunction implements Function<String, String> {

    @Override
    public String process(String input, Context context) throws Exception {
        PulsarAdmin adminClient = context.getPulsarAdmin();
        if (adminClient != null) {
            String topic = context.getCurrentRecord().getTopicName().isPresent() ?
                    context.getCurrentRecord().getTopicName().get() : null;
            String subName = context.getTenant() + "/" + context.getNamespace() + "/" + context.getFunctionName();
            if (topic != null) {
                // 1578188166 below is a random-pick timestamp
                adminClient.topics().resetCursor(topic, subName, 1578188166);
                return "reset cursor successfully";
            }
        }
        return null;
    }
}

```

To enable your function to get access to the Pulsar admin client, you need to set `exposeAdminClientEnabled=true` in the `conf/functions_worker.yml` file. To test whether it is enabled or not, you can use the command `pulsar-admin functions localrun` with the flag `--web-service-url` as follows.

```bash

bin/pulsar-admin functions localrun \
 --jar my-functions.jar \
 --classname my.package.CursorManagementFunction \
 --web-service-url http://pulsar-web-service:8080 \
 # Other function configs

```

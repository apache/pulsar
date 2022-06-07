---
id: functions-package-go
title: Package Go Functions
sidebar_label: "Package Go Functions"
---

:::note

Currently, Go functions can be implemented only using SDK and the interface of functions is exposed in the form of SDK. Before using Go functions, you need to import `github.com/apache/pulsar/pulsar-function-go/pf`. 

:::

To package a Go function, complete the following steps.

1. Prepare a Go function file.
2. Build the Go function.

   ```go

    go build <your Go function filename>.go

   ```

3. Copy the Go function file to the Pulsar image.

   ```bash

    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <your go function path>  CONTAINER ID:/pulsar

   ```

  4. Run the Go function with the following command.

   ```bash

    bin/pulsar-admin functions localrun \
        --go [your go function path] 
        --inputs [input topics] \
        --output [output topic] \
        --tenant [default:public] \
        --namespace [default:default] \
        --name [custom unique go function name] 

   ```

   The following log indicates that the Go function starts successfully.

   ```text

    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    ...

   ```

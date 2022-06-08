---
id: functions-package-python
title: Package Python Functions
sidebar_label: "Package Python Functions"
---

Python functions support the following three packaging formats:
- One Python file
- ZIP file
- PIP

## One Python file

To package a Python function into **one Python file**, complete the following steps.

1. Write a Python function.

   ```python

    from pulsar import Function //  import the Function module from Pulsar

    # The classic ExclamationFunction that appends an exclamation at the end
    # of the input
    class ExclamationFunction(Function):
      def __init__(self):
        pass

      def process(self, input, context):
        return input + '!'

   ```

    In this example, when you write a Python function, you need to inherit the Function class and implement the `process()` method.

    `process()` mainly has two parameters: 

    - `input` represents your input.
  
    - `context` represents an interface exposed by the Pulsar Function. You can get the attributes in the Python function based on the provided context object.

2. Install a Python client. The implementation of a Python function depends on the Python client. 

   ```bash

    pip install pulsar-client==2.10.0

   ```

3. Copy the Python function file to the Pulsar image.

   ```bash

    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <path of Python function file>  CONTAINER ID:/pulsar

   ```

4. Run the Python function using the following command.

   ```bash

    ./bin/pulsar-admin functions localrun \
    --classname <Python Function file name>.<Python Function class name> \
    --py <path of Python Function file> \
    --inputs persistent://public/default/my-topic-1 \
    --output persistent://public/default/test-1 \
    --tenant public \
    --namespace default \
    --name PythonFunction

   ```

   The following log indicates that the Python function starts successfully.

   ```text

    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    …

   ```

## ZIP file

To package a Python function into a **ZIP file**, complete the following steps.

1. Prepare the ZIP file. 

   ```text

    Assuming the zip file is named as `func.zip`, unzip the `func.zip` folder:
        "func/src"
        "func/requirements.txt"
        "func/deps"

   ```
    
   Take the [exclamation.zip](https://github.com/apache/pulsar/tree/master/tests/docker-images/latest-version-image/python-examples) file as an example. The internal structure of the example is as follows.

   ```text

    .
    ├── deps
    │   └── sh-1.12.14-py2.py3-none-any.whl
    └── src
        └── exclamation.py

   ```

2. Copy the ZIP file to the Pulsar image.

   ```bash

    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <path of ZIP file>  CONTAINER ID:/pulsar

   ```

3. Run the Python function using the following command.

   ```bash

    ./bin/pulsar-admin functions localrun \
    --classname exclamation \
    --py <path of ZIP file> \
    --inputs persistent://public/default/in-topic \
    --output persistent://public/default/out-topic \
    --tenant public \
    --namespace default \
    --name PythonFunction

   ```

    The following log indicates that the Python function starts successfully.

   ```text

    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    ...

   ```

## PIP

:::note

The PIP method is only supported in Kubernetes runtime. 

:::

To package a Python function with **PIP**, complete the following steps.

1. Configure the `functions_worker.yml` file.

   ```text

    #### Kubernetes Runtime ####
    installUserCodeDependencies: true

   ```

2. Write your Python Function.

   ```python

    from pulsar import Function
    import js2xml

    # The classic ExclamationFunction that appends an exclamation at the end
    # of the input
    class ExclamationFunction(Function):
     def __init__(self):
       pass

     def process(self, input, context):
      // add your logic
      return input + '!'

   ```

   You can introduce additional dependencies. When Python functions detect that the file currently used is `whl` and the `installUserCodeDependencies` parameter is specified, the system uses the `pip install` command to install the dependencies required in Python functions.

3. Generate the `whl` file.

   ```shell

    $ cd $PULSAR_HOME/pulsar-functions/scripts/python
    $ chmod +x generate.sh
    $ ./generate.sh <path of your Python Function> <path of the whl output dir> <the version of whl>
    # e.g: ./generate.sh /path/to/python /path/to/python/output 1.0.0

   ```

   The output is written in `/path/to/python/output`:

   ```text

    -rw-r--r--  1 root  staff   1.8K  8 27 14:29 pulsarfunction-1.0.0-py2-none-any.whl
    -rw-r--r--  1 root  staff   1.4K  8 27 14:29 pulsarfunction-1.0.0.tar.gz
    -rw-r--r--  1 root  staff     0B  8 27 14:29 pulsarfunction.whl

   ```

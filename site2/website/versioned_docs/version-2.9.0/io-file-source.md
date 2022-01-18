---
id: version-2.9.0-io-file-source
title: File source connector
sidebar_label: File source connector
original_id: io-file-source
---

The File source connector pulls messages from files in directories and persists the messages to Pulsar topics.

## Configuration

The configuration of the File source connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `inputDirectory` | String|true  | No default value|The input directory to pull files. |
| `recurse` | Boolean|false | true | Whether to pull files from subdirectory or not.|
| `keepFile` |Boolean|false | false | If set to true, the file is not deleted after it is processed, which means the file can be picked up continually. |
| `fileFilter` | String|false| [^\\.].* | The file whose name matches the given regular expression is picked up. |
| `pathFilter` | String |false | NULL | If `recurse` is set to true, the subdirectory whose path matches the given regular expression is scanned. |
| `minimumFileAge` | Integer|false | 0 | The minimum age that a file can be processed. <br><br>Any file younger than `minimumFileAge` (according to the last modification date) is ignored. |
| `maximumFileAge` | Long|false |Long.MAX_VALUE | The maximum age that a file can be processed. <br><br>Any file older than `maximumFileAge` (according to last modification date) is ignored. |
| `minimumSize` |Integer| false |1 | The minimum size (in bytes) that a file can be processed. |
| `maximumSize` | Double|false |Double.MAX_VALUE| The maximum size (in bytes) that a file can be processed. |
| `ignoreHiddenFiles` |Boolean| false | true| Whether the hidden files should be ignored or not. |
| `pollingInterval`|Long | false | 10000L | Indicates how long to wait before performing a directory listing. |
| `numWorkers` | Integer | false | 1 | The number of worker threads that process files.<br><br> This allows you to process a larger number of files concurrently. <br><br>However, setting this to a value greater than 1 makes the data from multiple files mixed in the target topic. |

### Example

Before using the File source connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "inputDirectory": "/Users/david",
        "recurse": true,
        "keepFile": true,
        "fileFilter": "[^\\.].*",
        "pathFilter": "*",
        "minimumFileAge": 0,
        "maximumFileAge": 9999999999,
        "minimumSize": 1,
        "maximumSize": 5000000,
        "ignoreHiddenFiles": true,
        "pollingInterval": 5000,
        "numWorkers": 1
    }
    ```

* YAML

    ```yaml
    configs:
        inputDirectory: "/Users/david"
        recurse: true
        keepFile: true
        fileFilter: "[^\\.].*"
        pathFilter: "*"
        minimumFileAge: 0
        maximumFileAge: 9999999999
        minimumSize: 1
        maximumSize: 5000000
        ignoreHiddenFiles: true
        pollingInterval: 5000
        numWorkers: 1
    ```

## Usage

Here is an example of using the File source connecter.

1. Pull a Pulsar image.

    ```bash
    $ docker pull apachepulsar/pulsar:{version}
    ```

2. Start Pulsar standalone.
   
    ```bash
    $ docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-standalone apachepulsar/pulsar:{version} bin/pulsar standalone
    ```

3. Create a configuration file _file-connector.yaml_.

    ```yaml
    configs:
        inputDirectory: "/opt"
    ```

4. Copy the configuration file _file-connector.yaml_ to the container.

    ```bash
    $ docker cp connectors/file-connector.yaml pulsar-standalone:/pulsar/
    ```

5. Download the File source connector.

    ```bash
    $ curl -O https://mirrors.tuna.tsinghua.edu.cn/apache/pulsar/pulsar-{version}/connectors/pulsar-io-file-{version}.nar
    ```

6. Start the File source connector.

    ```bash
    $ docker exec -it pulsar-standalone /bin/bash

    $ ./bin/pulsar-admin sources localrun \
    --archive /pulsar/pulsar-io-file-{version}.nar \
    --name file-test \
    --destination-topic-name  pulsar-file-test \
    --source-config-file /pulsar/file-connector.yaml
    ```

7. Start a consumer.

    ```bash
    ./bin/pulsar-client consume -s file-test -n 0 pulsar-file-test
    ```

8. Write the message to the file _test.txt_.
   
    ```bash
    echo "hello world!" > /opt/test.txt
    ```

    The following information appears on the consumer terminal window.

    ```bash
    ----- got message -----
    hello world!
    ```

    
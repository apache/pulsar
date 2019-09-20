---
id: io-file
title: File source connector
sidebar_label: File source connector
---

The File source connector pulls messages from files in directories and persists the messages to Pulsar topics.

## Configuration

The configuration of the File source connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `inputDirectory` | String|false|  | The input directory to pull files. |
| `recurse` | Boolean|false |  | Whether to pull files from subdirectory or not.|
| `keepFile` |Boolean|false | false | If set to true, the file is not deleted after it is processed, which means the file can be picked up continually. |
| `fileFilter` | String|false| [^\\.].* | The file whose name matches the given regular expression is picked up. |
| `pathFilter` | String |false |  | If `recurse` is set to true, the subdirectory whose path matches the given regular expression is scanned. |
| `minimumFileAge` | Integer|false |  | The minimum age that a file can be processed. <br><br>Any file younger than `minimumFileAge` (according to the last modification date) is ignored. |
| `maximumFileAge` | Long|false | | The maximum age that a file can be processed. <br><br>Any file older than `maximumFileAge` (according to last modification date) is ignored. |
| `minimumSize` |Integer| false | | The minimum size (in bytes) that a file can be processed. |
| `maximumSize` | Double|false || The maximum size (in bytes) that a file can be processed. |
| `ignoreHiddenFiles` |Boolean| false | | Whether the hidden files should be ignored or not. |
| `pollingInterval`|Long | false |  | Indicates how long to wait before performing a directory listing. |
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

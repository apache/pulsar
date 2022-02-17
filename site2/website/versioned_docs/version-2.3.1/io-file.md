---
id: io-file
title: File Connector
sidebar_label: "File Connector"
original_id: io-file
---

## Source

The File Source Connector is used to pull messages from files in a directory and persist the messages
to a Pulsar topic.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| inputDirectory | `true` | `null` | The input directory from which to pull files. |
| recurse | `false` | `true` | Indicates whether or not to pull files from sub-directories. |
| keepFile | `false` | `false` | If true, the file is not deleted after it has been processed and causes the file to be picked up continually. |
| fileFilter | `false` | `[^\\.].*` | Only files whose names match the given regular expression will be picked up. |
| pathFilter | `false` | `null` | When 'recurse' property is true, then only sub-directories whose path matches the given regular expression will be scanned. |
| minimumFileAge | `false` | `0` | The minimum age that a file must be in order to be processed; any file younger than this amount of time (according to last modification date) will be ignored. |
| maximumFileAge | `false` | `Long.MAX_VALUE` | The maximum age that a file must be in order to be processed; any file older than this amount of time (according to last modification date) will be ignored. |
| minimumSize | `false` | `1` | The minimum size (in bytes) that a file must be in order to be processed. |
| maximumSize | `false` | `Double.MAX_VALUE` | The maximum size (in bytes) that a file can be in order to be processed. |
| ignoreHiddenFiles | `false` | `true` | Indicates whether or not hidden files should be ignored or not. |
| pollingInterval | `false` | `10000` | Indicates how long to wait before performing a directory listing. |
| numWorkers | `false` | `1` | The number of worker threads that will be processing the files. This allows you to process a larger number of files concurrently. However, setting this to a value greater than 1 will result in the data from multiple files being "intermingled" in the target topic. |
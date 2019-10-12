---
id: sql-rest-api
title: Pulsar SQL REST APIs
sidebar_label: REST APIs
---

## Presto HTTP API

Presto configuration in Pulsar is: conf/presto/config.properties

Get cluster status (HTTP method: GET)

```
localhost:8081/v1/cluster
```

You can input your SQL as an HTTP request body, and send it to `localhost:8081/v1/statement`.

For example:

```
HTTP request URL : `localhost:8081/v1/statement`
HTTP header:  X-Presto-User: username
HTTP body: `show catalogs`
```

> For more HTTP header, refer to https://github.com/prestosql/presto/blob/master/presto-client/src/main/java/io/prestosql/client/PrestoHeaders.java ）


You will receive information like this:
```
{
    "id": "20190522_083232_00012_tk5nc",
    "infoUri": "http://localhost:8081/ui/query.html?20190522_083232_00012_tk5nc",
    "nextUri": "http://localhost:8081/v1/statement/20190522_083232_00012_tk5nc/1",
    "stats": {
        "state": "QUEUED",
        "queued": true,
        "scheduled": false,
        "nodes": 0,
        "totalSplits": 0,
        "queuedSplits": 0,
        "runningSplits": 0,
        "completedSplits": 0,
        "userTimeMillis": 0,
        "cpuTimeMillis": 0,
        "wallTimeMillis": 0,
        "queuedTimeMillis": 0,
        "elapsedTimeMillis": 0,
        "processedRows": 0,
        "processedBytes": 0,
        "peakMemoryBytes": 0
    }
}
```

Then you need to request the `nextUri` using the GET method, and you will receive information like this:
```
{
    "id": "20190522_083232_00012_tk5nc",
    "infoUri": "http://localhost:8081/ui/query.html?20190522_083232_00012_tk5nc",
    "nextUri": "http://localhost:8081/v1/statement/20190522_083232_00012_tk5nc/2",
    "stats": {
        "state": "PLANNING",
        "queued": false,
        "scheduled": false,
        "nodes": 0,
        "totalSplits": 0,
        "queuedSplits": 0,
        "runningSplits": 0,
        "completedSplits": 0,
        "userTimeMillis": 0,
        "cpuTimeMillis": 0,
        "wallTimeMillis": 0,
        "queuedTimeMillis": 1,
        "elapsedTimeMillis": 1,
        "processedRows": 0,
        "processedBytes": 0,
        "peakMemoryBytes": 0
    }
}
```
Then send the `nextUri` request again, and you will receive the target data.
```
{
    "id": "20190522_083232_00012_tk5nc",
    "infoUri": "http://localhost:8081/ui/query.html?20190522_083232_00012_tk5nc",
    "columns": [
        {
            "name": "Catalog",
            "type": "varchar(6)",
            "typeSignature": {
                "rawType": "varchar",
                "typeArguments": [],
                "literalArguments": [],
                "arguments": [
                    {
                        "kind": "LONG_LITERAL",
                        "value": 6
                    }
                ]
            }
        }
    ],
    "data": [
        [
            "pulsar"
        ],
        [
            "system"
        ]
    ],
    "stats": {
        "state": "FINISHED",
        "queued": false,
        "scheduled": true,
        "nodes": 1,
        "totalSplits": 19,
        "queuedSplits": 0,
        "runningSplits": 0,
        "completedSplits": 19,
        "userTimeMillis": 10,
        "cpuTimeMillis": 12,
        "wallTimeMillis": 122,
        "queuedTimeMillis": 1,
        "elapsedTimeMillis": 101,
        "processedRows": 0,
        "processedBytes": 0,
        "peakMemoryBytes": 0,
        "rootStage": {
            "stageId": "0",
            "state": "FINISHED",
            "done": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "userTimeMillis": 1,
            "cpuTimeMillis": 1,
            "wallTimeMillis": 5,
            "processedRows": 2,
            "processedBytes": 22,
            "subStages": [
                {
                    "stageId": "1",
                    "state": "FINISHED",
                    "done": true,
                    "nodes": 1,
                    "totalSplits": 17,
                    "queuedSplits": 0,
                    "runningSplits": 0,
                    "completedSplits": 17,
                    "userTimeMillis": 8,
                    "cpuTimeMillis": 10,
                    "wallTimeMillis": 111,
                    "processedRows": 2,
                    "processedBytes": 51,
                    "subStages": [
                        {
                            "stageId": "2",
                            "state": "FINISHED",
                            "done": true,
                            "nodes": 1,
                            "totalSplits": 1,
                            "queuedSplits": 0,
                            "runningSplits": 0,
                            "completedSplits": 1,
                            "userTimeMillis": 1,
                            "cpuTimeMillis": 1,
                            "wallTimeMillis": 6,
                            "processedRows": 2,
                            "processedBytes": 22,
                            "subStages": []
                        }
                    ]
                }
            ]
        },
        "progressPercentage": 100
    }
}
```

If you want to query schemas, catalogs or other information in Pulsar, replace the request body with select SQL.

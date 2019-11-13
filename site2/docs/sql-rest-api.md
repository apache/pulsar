---
id: sql-rest-api
title: Pulsar SQL REST APIs
sidebar_label: REST APIs
---

## Overview

This describes the resources that make up the Presto REST API v1. If you have any problems or request, please create a Github issue to contact us.

## Current version

All requests should use the v1 version of REST API to request the presto service. You need to use explicitly URL `http://presto.service:8081/v1` to request service.

Also, the header `X-Presto-User` is required when you send a `POST` request.

```properties
X-Presto-User: username
```

For more information about headers, see the [presto/PrestoHeaders.java at master · prestodb/presto · GitHub](https://github.com/prestodb/presto/blob/master/presto-client/src/main/java/com/facebook/presto/client/PrestoHeaders.java)

## Schema

You can type statement in the HTTP body. And all data is received as JSON that might contain a `nextUri` link. If there is no `nextUri` link, then the query is finished (either successfully completed or failed). Otherwise., keep following the `nextUri` link.

**Example**: Execute `show catalogs`.

```powershell
➜  ~ curl --header "X-Presto-User: test-user" --request POST --data 'show catalogs' http://localhost:8081/v1/statement | json_pp
{
   "infoUri" : "http://localhost:8081/ui/query.html?20191113_033653_00006_dg6hb",
   "stats" : {
      "queued" : true,
      "nodes" : 0,
      "userTimeMillis" : 0,
      "cpuTimeMillis" : 0,
      "wallTimeMillis" : 0,
      "processedBytes" : 0,
      "processedRows" : 0,
      "runningSplits" : 0,
      "queuedTimeMillis" : 0,
      "queuedSplits" : 0,
      "completedSplits" : 0,
      "totalSplits" : 0,
      "scheduled" : false,
      "peakMemoryBytes" : 0,
      "state" : "QUEUED",
      "elapsedTimeMillis" : 0
   },
   "id" : "20191113_033653_00006_dg6hb",
   "nextUri" : "http://localhost:8081/v1/statement/20191113_033653_00006_dg6hb/1"
}

➜  ~ curl http://localhost:8081/v1/statement/20191113_033653_00006_dg6hb/1 | json_pp
{
   "infoUri" : "http://localhost:8081/ui/query.html?20191113_033653_00006_dg6hb",
   "nextUri" : "http://localhost:8081/v1/statement/20191113_033653_00006_dg6hb/2",
   "id" : "20191113_033653_00006_dg6hb",
   "stats" : {
      "state" : "PLANNING",
      "totalSplits" : 0,
      "queued" : false,
      "userTimeMillis" : 0,
      "completedSplits" : 0,
      "scheduled" : false,
      "wallTimeMillis" : 0,
      "runningSplits" : 0,
      "queuedSplits" : 0,
      "cpuTimeMillis" : 0,
      "processedRows" : 0,
      "processedBytes" : 0,
      "nodes" : 0,
      "queuedTimeMillis" : 1,
      "elapsedTimeMillis" : 2,
      "peakMemoryBytes" : 0
   }
}

➜  ~ curl http://localhost:8081/v1/statement/20191113_033653_00006_dg6hb/2 | json_pp
{
   "id" : "20191113_033653_00006_dg6hb",
   "data" : [
      [
         "pulsar"
      ],
      [
         "system"
      ]
   ],
   "infoUri" : "http://localhost:8081/ui/query.html?20191113_033653_00006_dg6hb",
   "columns" : [
      {
         "typeSignature" : {
            "rawType" : "varchar",
            "arguments" : [
               {
                  "kind" : "LONG_LITERAL",
                  "value" : 6
               }
            ],
            "literalArguments" : [],
            "typeArguments" : []
         },
         "name" : "Catalog",
         "type" : "varchar(6)"
      }
   ],
   "stats" : {
      "wallTimeMillis" : 104,
      "scheduled" : true,
      "userTimeMillis" : 14,
      "progressPercentage" : 100,
      "totalSplits" : 19,
      "nodes" : 1,
      "cpuTimeMillis" : 16,
      "queued" : false,
      "queuedTimeMillis" : 1,
      "state" : "FINISHED",
      "peakMemoryBytes" : 0,
      "elapsedTimeMillis" : 111,
      "processedBytes" : 0,
      "processedRows" : 0,
      "queuedSplits" : 0,
      "rootStage" : {
         "cpuTimeMillis" : 1,
         "runningSplits" : 0,
         "state" : "FINISHED",
         "completedSplits" : 1,
         "subStages" : [
            {
               "cpuTimeMillis" : 14,
               "runningSplits" : 0,
               "state" : "FINISHED",
               "completedSplits" : 17,
               "subStages" : [
                  {
                     "wallTimeMillis" : 7,
                     "subStages" : [],
                     "stageId" : "2",
                     "done" : true,
                     "nodes" : 1,
                     "totalSplits" : 1,
                     "processedBytes" : 22,
                     "processedRows" : 2,
                     "queuedSplits" : 0,
                     "userTimeMillis" : 1,
                     "cpuTimeMillis" : 1,
                     "runningSplits" : 0,
                     "state" : "FINISHED",
                     "completedSplits" : 1
                  }
               ],
               "wallTimeMillis" : 92,
               "nodes" : 1,
               "done" : true,
               "stageId" : "1",
               "userTimeMillis" : 12,
               "processedRows" : 2,
               "processedBytes" : 51,
               "queuedSplits" : 0,
               "totalSplits" : 17
            }
         ],
         "wallTimeMillis" : 5,
         "done" : true,
         "nodes" : 1,
         "stageId" : "0",
         "userTimeMillis" : 1,
         "processedRows" : 2,
         "processedBytes" : 22,
         "totalSplits" : 1,
         "queuedSplits" : 0
      },
      "runningSplits" : 0,
      "completedSplits" : 19
   }
}
```



## Response JSON field

- `status`

The `status` field is only for displaying to humans as a hint about the query’s state on the server. 

- `error`

The `error` field is for distinguishing between a successfully completed query and a failed query when there is no more `nextUri` link.


## More information

For more information about Presto REST API. See the https://github.com/prestodb/presto/wiki/HTTP-Protocol

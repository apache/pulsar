---
id: version-2.6.4-sql-rest-api
title: Pulsar SQL REST APIs
sidebar_label: REST APIs
original_id: sql-rest-api
---

This section lists resources that make up the Presto REST API v1. 

## Request for Presto services

All requests for Presto services should use Presto REST API v1 version. 

To request services, use explicit URL `http://presto.service:8081/v1`. You need to update `presto.service:8081` with your real Presto address before sending requests.

`POST` requests require the `X-Presto-User` header. If you use authentication, you must use the same `username` that is specified in the authentication configuration. If you do not use authentication, you can specify anything for `username`.

```properties
X-Presto-User: username
```

For more information about headers, refer to [PrestoHeaders](https://github.com/trinodb/trino).

## Schema

You can use statement in the HTTP body. All data is received as JSON document that might contain a `nextUri` link. If the received JSON document contains a `nextUri` link, the request continues with the `nextUri` link until the received data does not contain a `nextUri` link. If no error is returned, the query completes successfully. If an `error` field is displayed in `stats`, it means the query fails.

The following is an example of `show catalogs`. The query continues until the received JSON document does not contain a `nextUri` link. Since no `error` is displayed in `stats`, it means that the query completes successfully.

```powershell
➜  ~ curl --header "X-Presto-User: test-user" --request POST --data 'show catalogs' http://localhost:8081/v1/statement
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

➜  ~ curl http://localhost:8081/v1/statement/20191113_033653_00006_dg6hb/1
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

➜  ~ curl http://localhost:8081/v1/statement/20191113_033653_00006_dg6hb/2
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

> Note
> 
> Since the response data is not in sync with the query state from the perspective of clients, you cannot rely on the response data to determine whether the query completes.

For more information about Presto REST API, refer to [Presto HTTP Protocol](https://github.com/prestosql/presto/wiki/HTTP-Protocol).

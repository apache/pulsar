---
id: sql-getting-started
title: Query data with Pulsar SQL
sidebar_label: "Query data"
---

Before querying data in Pulsar, you need to install Pulsar and built-in connectors.

## Requirements

1. Install [Pulsar](getting-started-standalone.md).
2. Install Pulsar [built-in connectors](io-quickstart.md#install-pulsar-and-built-in-connector).

## Query data in Pulsar

To query data in Pulsar with Pulsar SQL, complete the following steps.

1. Start a Pulsar standalone cluster:

```bash
PULSAR_STANDALONE_USE_ZOOKEEPER=1 ./bin/pulsar standalone
```

:::note

Starting the Pulsar standalone cluster from scratch doesn't enable ZooKeeper by default. However, the Pulsar SQL depends on ZooKeeper. Therefore, you need to set `PULSAR_STANDALONE_USE_ZOOKEEPER=1` to enable ZooKeeper.

:::

2. Start a Pulsar SQL worker:

```bash
./bin/pulsar sql-worker run
```

3. After initializing Pulsar standalone cluster and the SQL worker, run SQL CLI:

```bash
./bin/pulsar sql
```

4. Test with SQL commands:

```bash
trino> show catalogs;
 Catalog
---------
 pulsar
 system
(2 rows)

Query 20180829_211752_00004_7qpwh, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]


trino> show schemas in pulsar;
        Schema
-----------------------
 information_schema
 public/default
 public/functions
(3 rows)

Query 20180829_211818_00005_7qpwh, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [4 rows, 89B] [21 rows/s, 471B/s]


trino> show tables in pulsar."public/default";
 Table
-------
(0 rows)

Query 20180829_211839_00006_7qpwh, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

Since there is no data in Pulsar, no records are returned.

5. Start the built-in connector `DataGeneratorSource` and ingest some mock data:

```bash
./bin/pulsar-admin sources create --name generator --destinationTopicName generator_test --source-type data-generator
```

And then you can query a topic in the namespace "public/default":

```bash
trino> show tables in pulsar."public/default";
     Table
----------------
 generator_test
(1 row)

Query 20180829_213202_00000_csyeu, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:02 [1 rows, 38B] [0 rows/s, 17B/s]
```

You can now query the data within the topic "generator_test":

```bash
trino> select * from pulsar."public/default".generator_test;

  firstname  | middlename  |  lastname   |              email               |   username   | password | telephonenumber | age |                 companyemail                  | nationalidentitycardnumber |
-------------+-------------+-------------+----------------------------------+--------------+----------+-----------------+-----+-----------------------------------------------+----------------------------+
 Genesis     | Katherine   | Wiley       | genesis.wiley@gmail.com          | genesisw     | y9D2dtU3 | 959-197-1860    |  71 | genesis.wiley@interdemconsulting.eu           | 880-58-9247                |
 Brayden     |             | Stanton     | brayden.stanton@yahoo.com        | braydens     | ZnjmhXik | 220-027-867     |  81 | brayden.stanton@supermemo.eu                  | 604-60-7069                |
 Benjamin    | Julian      | Velasquez   | benjamin.velasquez@yahoo.com     | benjaminv    | 8Bc7m3eb | 298-377-0062    |  21 | benjamin.velasquez@hostesltd.biz              | 213-32-5882                |
 Michael     | Thomas      | Donovan     | donovan@mail.com                 | michaeld     | OqBm9MLs | 078-134-4685    |  55 | michael.donovan@memortech.eu                  | 443-30-3442                |
 Brooklyn    | Avery       | Roach       | brooklynroach@yahoo.com          | broach       | IxtBLafO | 387-786-2998    |  68 | brooklyn.roach@warst.biz                      | 085-88-3973                |
 Skylar      |             | Bradshaw    | skylarbradshaw@yahoo.com         | skylarb      | p6eC6cKy | 210-872-608     |  96 | skylar.bradshaw@flyhigh.eu                    | 453-46-0334                |
...
```

You can query the mock data.

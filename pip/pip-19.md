# PIP-19: Pulsar SQL

* **Status**: In Progress
* **Authors**: Jerry Peng, Matteo Merli
* **Mailing List discussion**: 
N/A
* **Prototype**: https://github.com/jerrypeng/presto/tree/pulsar_connector

## What are we trying to do?

We are trying to create a method in which users can explore, in a natural manner,  the data already stored within Pulsar topics.  We believe the best way to accomplish this is to expose SQL interface that allows users to query existing data within a Pulsar cluster.  

Just to be absolutely clear,  the SQL we are proposing is for querying data already in Pulsar and we are currently not proposing the implementation of any sort of SQL on data streams


## Why are we doing this?

Many users are interested in such a feature.  For example, many users store large amounts of historical data in Pulsar for various purposes.  Giving them to capability to query that that data gives them huge value.  Users will typically need to stream the data out of Pulsar and into another platform to do any sort of analysis, but with Pulsar SQL, users can just use one platform.

## How are we going to do it?

With the implementation of a schema registry in Pulsar, data can be structured so that it can be easily mapped to tables that can be queried by SQL. We plan on using Presto (https://prestodb.io/) as the backbone of Pulsar SQL.  A connector can be implemented using the Presto connector SPI that allows presto to ingest data from Pulsar and to be queried using Presto’s existing SQL framework.

The schema registry will be used to generate the structure of tables that will be used in Presto.  Presto workers will load data directly from bookies through a read-only managed-ledger interface, so that we can have a many to many throughput and avoid impacting brokers with read activity.

Thus, Pulsar will be queried for metadata concerning topics and schemas and from that metadata, we will go directly to the bookies to load and deserialize the data.


## Goals

* Allow users to submit SQL queries using a Pulsar CLI
* Throttling
	* Maintain SLAs between reads for SQL and consumer reads
* Resource Isolation
	* Read priorities at BookKeeper level - Bookies should be able to support different classes of read requests and prioritize regular consumers vs read requests coming from SQL queries
	* Queries from users or groups should not overly impact other queries from other users or groups
* We would like to target two types of deployments:
	1. Embedded with pulsar cluster
		* Run Presto coordinator and workers as part of the existing function worker service. 
	2. Allow existing presto clusters to be used to query a Pulsar cluster

## Implementation

### Presto-Pulsar connector

Presto has a SPI system to add connectors. We need to create a connector able to perform the following tasks: 
* Discover schema for a particular topic
	* This will be done through Pulsar Schema REST API
* Discover the amount of entries stored in a topic and break down the entries across multiple workers
* On each worker we need to fetch the entries from Bookies
	* Use managed ledger in read-only mode
	* Each worker positions itself on a particular entry and read a determined number of entries
	* Parse Pulsar message metadata and extract messages from the BookKeeper entries
	* Deserialize entries, based on schema definition, and feed the objects to Presto
	
    
### Presto deployment

Presto is composed of a coordination and multiple worker nodes (https://prestodb.io/overview.html). We plan to have Presto run in embedded mode with regular Pulsar scripts for uniform operations. 

One of the nodes will be elected as coordinator (in the same way as function workers elect a leader already). Since all the requests to Presto coordinator are made through HTTP/HTTPS, it would be possible to proxy/redirect requests made through the regular Pulsar service URL. 

Regarding worker nodes, one possibility is to co-locate with Pulsar function worker nodes, which can in their turn be co-located with brokers, when deploying on a small single-tenant cluster.

## Execution Plan

Let’s break the implementation into multiple phases:

### Phase 1 (target Pulsar 2.2)
1. Implement Pulsar connector for Presto
	* Have basic queries working
2. Work with all schemas
	* JSON, Protobuf, Avro, String, etc.
3. Basic integration Test
4. Standalone pulsar and standalone presto
5. Deployment
	* Deploy alongside pulsar / embedded with pulsar cluster
6. Metrics
	* Prometheus
	* How long queries are taking, etc.
	* Number of requests per sec, etc.
7. System Columns
	* Automatically generate columns such as publish time, event time, partitionId, etc for each table
8. Integrate with pulsar CLI
9. Integration Tiered Storage
	* Pass credentials e.g. S3


### Phase2

1. Resource Isolation/Management
2. Throttling
3. Priorities
4. Time boxed queries
5. When doing a query over a subset of the data, based on publish time, we should be able to only scan the relevant data instead of everything stored in the topic
6. Performance testing and optimizing


More to come...

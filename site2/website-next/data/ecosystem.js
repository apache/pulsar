module.exports = {
    connectors: [{
            name: 'ActiveMQ Sink',
            description: 'The ActiveMQ sink connector pulls messages from Pulsar topics and persist messages to ActiveMQ clusters.',
            link: 'https://hub.streamnative.io/connectors/activemq-sink/2.5.1/'
        },
        {
            name: 'ActiveMQ Source',
            description: 'The ActiveMQ source connector receives messages from ActiveMQ clusters and writes messages to Pulsar topics.',
            link: 'https://hub.streamnative.io/connectors/activemq-source/2.5.1'
        },
        {
            name: 'Aerospike',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#aerospike'

        },
        {
            name: 'Apache Geode',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/geode'

        },
        {
            name: 'Apache Kudu',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/kudu'

        },
        {
            name: 'Apache Phoenix',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/phoenix'

        },
        {
            name: 'Apache PLC4X',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/plc4x'

        },
        {
            name: 'Azure DocumentDB',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/azure-documentdb'

        },
        {
            name: 'Canal Source',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#canal'
        },
        {
            name: 'Cassandra Sink',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#cassandra'
        },
        {
            name: 'CoAP',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/coap'
        },
        {
            name: 'Couchbase',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/couchbase'
        },
        {
            name: 'DataDog Logs',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/datadog'
        },
        {
            name: 'Diffusion',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/diffusion'
        },
        {
            name: 'Datastax Snowflake Sink Connector for Apache Pulsar',
            description: '',
            link: 'https://github.com/datastax/snowflake-connector'
        },
        {
            name: 'Debezium Microsoft SQL Server',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#debezium-microsoft-sql-server'
        },
        {
            name: 'Debezium MongoDB',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#debezium-mongodb'
        },
        {
            name: 'Debezium MySQL',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#debezium-mysql'
        },
        {
            name: 'Debezium Oracle',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#debezium-oracle'
        },
        {
            name: 'Debezium PostgreSQL',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#debezium-postgresql'
        },
        {
            name: 'DynamoDB',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#dynamodb'
        },
        {
            name: 'ElasticSearch',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#elasticsearch'
        },
        {
            name: 'File',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#file'
        },
        {
            name: 'Flume',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#flume-1'
        },
        {
            name: 'Flume',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#flume'
        },
        {
            name: 'Google BigQuery',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/bigquery'
        },
        {
            name: 'Hazelcast Jet',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/hazelcast'
        },
        {
            name: 'HBase',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#hbase'
        },
        {
            name: 'HDFS2',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#hdfs2'
        },
        {
            name: 'HDFS3',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#hdfs3'
        },
        {
            name: 'Humio HEC',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/humio'
        },
        {
            name: 'InfluxDB',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#influxdb'
        },
        {
            name: 'JDBC ClickHouse',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#jdbc-clickhouse'
        },
        {
            name: 'JDBC MariaDB',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#jdbc-mariadb'
        },
        {
            name: 'JDBC PostgreSQL',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#jdbc-postgresql'

        },
        {
            name: 'JDBC SQLite',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#jdbc-sqlite'

        },
        {
            name: 'JMS',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/jms'

        },
        {
            name: 'Kafka',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#kafka-1'

        },
        {
            name: 'Kafka',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#kafka'

        },
        {
            name: 'Kinesis',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#kinesis-1'

        },
        {
            name: 'Kinesis',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#kinesis'

        },
        {
            name: 'Kinetica',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/kinetica'

        },
        {
            name: 'MarkLogic',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/marklogic'

        },
        {
            name: 'MQTT',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/mqtt'

        },
        {
            name: 'MongoDB',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#mongodb'

        },
        {
            name: 'Netty',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#netty'
        },
        {
            name: 'Neo4J',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/neo4j'
        },
        {
            name: 'New Relic',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/newrelic'
        },
        {
            name: 'NSQ',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#nsq'
        },
        {
            name: 'OrientDB',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/orientdb'
        },
        {
            name: 'RabbitMQ',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#rabbitmq-1'
        },
        {
            name: 'RabbitMQ',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#rabbitmq'
        },
        {
            name: 'Redis',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#redis'
            
        },
        {
            name: 'Redis',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/redis'
            
        },
        {
            name: 'SAP HANA',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/sap-hana'
        },
        {
            name: 'SingleStore',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/singlestore'
        },
        {
            name: 'Solr',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#solr'
        },
        {
            name: 'Splunk',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/splunk'
        },
        {
            name: 'Twitter firehose',
            description: '',
            link: 'https://pulsar.apache.org/docs/en/io-connectors/#twitter-firehose'
        },
        {
            name: 'XTDB',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/xtdb'
        },
        {
            name: 'Zeebe',
            description: '',
            link: 'https://github.com/datastax/pulsar-3rdparty-connector/blob/master/pulsar-connectors/zeebe'
        },
    ],
    tools: [{
            name: 'DataStax CDC for Apache Cassandra',
            description: 'Send Apache Cassandra mutations for tables having Change Data Capture (CDC) enabled to Luna Streaming or Apache Pulsar™, which in turn can write the data to platforms such as Elasticsearch® or Snowflake®.',
            link: 'https://github.com/datastax/cdc-apache-cassandra'
        },
        {
            name: 'Helm Chart for Apache Pulsar',
            description: '',
            link: 'https://github.com/datastax/pulsar-helm-chart'
        },
        {
            name: 'Pulsar Admin Console',
            description: 'A web based UI that administrates topics, namespaces, sources, sinks and various aspects of Apache Pulsar features.',
            link: 'https://github.com/datastax/pulsar-admin-console'
        },
        {
            name: 'Starlight for JMS',
            description: 'Implements the JMS 2.0 (Java Messaging Service ®) API over the Apache Pulsar® Java Client.',
            link: 'https://github.com/datastax/starlight-for-kafka'
        },
        {
            name: 'Starlight for RabbitMQ',
            description: 'Acts as a proxy between your RabbitMQ application and Apache Pulsar. ',
            link: 'https://github.com/datastax/starlight-for-rabbitmq'
        },
    ],
    adapters: [{
            name: 'Pulsar Client Kafka Compatible',
            description: '',
            link: 'https://github.com/apache/pulsar-adapters/tree/master/pulsar-client-kafka-compat'
        },
        {
            name: 'Pulsar Flink',
            description: '',
            link: 'https://github.com/apache/pulsar-adapters/tree/master/pulsar-flink'
        },
        {
            name: 'Pulsar Log4j2 Appender',
            description: '',
            link: 'https://github.com/apache/pulsar-adapters/tree/master/pulsar-log4j2-appender'
        },
        {
            name: 'Pulsar Spark',
            description: '',
            link: 'https://github.com/apache/pulsar-adapters/tree/master/pulsar-spark'
        },
        {
            name: 'Pulsar Storm',
            description: '',
            link: 'https://github.com/apache/pulsar-adapters/tree/master/pulsar-storm'
        }
    ],
}
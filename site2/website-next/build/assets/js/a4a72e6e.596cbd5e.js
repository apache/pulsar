"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[33872],{3905:function(a,e,t){t.d(e,{Zo:function(){return c},kt:function(){return k}});var r=t(67294);function i(a,e,t){return e in a?Object.defineProperty(a,e,{value:t,enumerable:!0,configurable:!0,writable:!0}):a[e]=t,a}function n(a,e){var t=Object.keys(a);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(a);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(a,e).enumerable}))),t.push.apply(t,r)}return t}function l(a){for(var e=1;e<arguments.length;e++){var t=null!=arguments[e]?arguments[e]:{};e%2?n(Object(t),!0).forEach((function(e){i(a,e,t[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(a,Object.getOwnPropertyDescriptors(t)):n(Object(t)).forEach((function(e){Object.defineProperty(a,e,Object.getOwnPropertyDescriptor(t,e))}))}return a}function o(a,e){if(null==a)return{};var t,r,i=function(a,e){if(null==a)return{};var t,r,i={},n=Object.keys(a);for(r=0;r<n.length;r++)t=n[r],e.indexOf(t)>=0||(i[t]=a[t]);return i}(a,e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(a);for(r=0;r<n.length;r++)t=n[r],e.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(a,t)&&(i[t]=a[t])}return i}var s=r.createContext({}),u=function(a){var e=r.useContext(s),t=e;return a&&(t="function"==typeof a?a(e):l(l({},e),a)),t},c=function(a){var e=u(a.components);return r.createElement(s.Provider,{value:e},a.children)},p={inlineCode:"code",wrapper:function(a){var e=a.children;return r.createElement(r.Fragment,{},e)}},m=r.forwardRef((function(a,e){var t=a.components,i=a.mdxType,n=a.originalType,s=a.parentName,c=o(a,["components","mdxType","originalType","parentName"]),m=u(t),k=i,h=m["".concat(s,".").concat(k)]||m[k]||p[k]||n;return t?r.createElement(h,l(l({ref:e},c),{},{components:t})):r.createElement(h,l({ref:e},c))}));function k(a,e){var t=arguments,i=e&&e.mdxType;if("string"==typeof a||i){var n=t.length,l=new Array(n);l[0]=m;var o={};for(var s in e)hasOwnProperty.call(e,s)&&(o[s]=e[s]);o.originalType=a,o.mdxType="string"==typeof a?a:i,l[1]=o;for(var u=2;u<n;u++)l[u]=t[u];return r.createElement.apply(null,l)}return r.createElement.apply(null,t)}m.displayName="MDXCreateElement"},18016:function(a,e,t){t.r(e),t.d(e,{frontMatter:function(){return o},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return c},default:function(){return m}});var r=t(87462),i=t(63366),n=(t(67294),t(3905)),l=["components"],o={id:"io-connectors",title:"Built-in connector",sidebar_label:"Built-in connector"},s=void 0,u={unversionedId:"io-connectors",id:"io-connectors",isDocsHomePage:!1,title:"Built-in connector",description:"Pulsar distribution includes a set of common connectors that have been packaged and tested with the rest of Apache Pulsar. These connectors import and export data from some of the most commonly used data systems.",source:"@site/docs/io-connectors.md",sourceDirName:".",slug:"/io-connectors",permalink:"/docs/next/io-connectors",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/docs/io-connectors.md",tags:[],version:"current",frontMatter:{id:"io-connectors",title:"Built-in connector",sidebar_label:"Built-in connector"},sidebar:"docsSidebar",previous:{title:"Debug",permalink:"/docs/next/io-debug"},next:{title:"CDC connector",permalink:"/docs/next/io-cdc"}},c=[{value:"Source connector",id:"source-connector",children:[{value:"Canal",id:"canal",children:[]},{value:"Debezium MySQL",id:"debezium-mysql",children:[]},{value:"Debezium PostgreSQL",id:"debezium-postgresql",children:[]},{value:"Debezium MongoDB",id:"debezium-mongodb",children:[]},{value:"Debezium Oracle",id:"debezium-oracle",children:[]},{value:"Debezium Microsoft SQL Server",id:"debezium-microsoft-sql-server",children:[]},{value:"DynamoDB",id:"dynamodb",children:[]},{value:"File",id:"file",children:[]},{value:"Flume",id:"flume",children:[]},{value:"Twitter firehose",id:"twitter-firehose",children:[]},{value:"Kafka",id:"kafka",children:[]},{value:"Kinesis",id:"kinesis",children:[]},{value:"Netty",id:"netty",children:[]},{value:"NSQ",id:"nsq",children:[]},{value:"RabbitMQ",id:"rabbitmq",children:[]}]},{value:"Sink connector",id:"sink-connector",children:[{value:"Aerospike",id:"aerospike",children:[]},{value:"Cassandra",id:"cassandra",children:[]},{value:"ElasticSearch",id:"elasticsearch",children:[]},{value:"Flume",id:"flume-1",children:[]},{value:"HBase",id:"hbase",children:[]},{value:"HDFS2",id:"hdfs2",children:[]},{value:"HDFS3",id:"hdfs3",children:[]},{value:"InfluxDB",id:"influxdb",children:[]},{value:"JDBC ClickHouse",id:"jdbc-clickhouse",children:[]},{value:"JDBC MariaDB",id:"jdbc-mariadb",children:[]},{value:"JDBC PostgreSQL",id:"jdbc-postgresql",children:[]},{value:"JDBC SQLite",id:"jdbc-sqlite",children:[]},{value:"Kafka",id:"kafka-1",children:[]},{value:"Kinesis",id:"kinesis-1",children:[]},{value:"MongoDB",id:"mongodb",children:[]},{value:"RabbitMQ",id:"rabbitmq-1",children:[]},{value:"Redis",id:"redis",children:[]},{value:"Solr",id:"solr",children:[]}]}],p={toc:c};function m(a){var e=a.components,t=(0,i.Z)(a,l);return(0,n.kt)("wrapper",(0,r.Z)({},p,t,{components:e,mdxType:"MDXLayout"}),(0,n.kt)("p",null,"Pulsar distribution includes a set of common connectors that have been packaged and tested with the rest of Apache Pulsar. These connectors import and export data from some of the most commonly used data systems. "),(0,n.kt)("p",null,"Using any of these connectors is as easy as writing a simple connector and running the connector locally or submitting the connector to a Pulsar Functions cluster."),(0,n.kt)("h2",{id:"source-connector"},"Source connector"),(0,n.kt)("p",null,"Pulsar has various source connectors, which are sorted alphabetically as below."),(0,n.kt)("h3",{id:"canal"},"Canal"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-canal-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-canal-source#usage"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/canal/src/main/java/org/apache/pulsar/io/canal/CanalStringSource.java"},"Java class")))),(0,n.kt)("h3",{id:"debezium-mysql"},"Debezium MySQL"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#example-of-mysql"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/mysql/src/main/java/org/apache/pulsar/io/debezium/mysql/DebeziumMysqlSource.java"},"Java class")))),(0,n.kt)("h3",{id:"debezium-postgresql"},"Debezium PostgreSQL"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#example-of-postgresql"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/postgres/src/main/java/org/apache/pulsar/io/debezium/postgres/DebeziumPostgresSource.java"},"Java class")))),(0,n.kt)("h3",{id:"debezium-mongodb"},"Debezium MongoDB"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#example-of-mongodb"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/mongodb/src/main/java/org/apache/pulsar/io/debezium/mongodb/DebeziumMongoDbSource.java"},"Java class")))),(0,n.kt)("h3",{id:"debezium-oracle"},"Debezium Oracle"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#example-of-oracle"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/oracle/src/main/java/org/apache/pulsar/io/debezium/oracle/DebeziumOracleSource.java"},"Java class")))),(0,n.kt)("h3",{id:"debezium-microsoft-sql-server"},"Debezium Microsoft SQL Server"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-debezium-source#example-of-microsoft-sql"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/mssql/src/main/java/org/apache/pulsar/io/debezium/mssql/DebeziumMsSqlSource.java"},"Java class")))),(0,n.kt)("h3",{id:"dynamodb"},"DynamoDB"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-dynamodb-source#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/dynamodb/src/main/java/org/apache/pulsar/io/dynamodb/DynamoDBSource.java"},"Java class"))),(0,n.kt)("h3",{id:"file"},"File"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-file-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-file-source#usage"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/file/src/main/java/org/apache/pulsar/io/file/FileSource.java"},"Java class")))),(0,n.kt)("h3",{id:"flume"},"Flume"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-flume-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/java/org/apache/pulsar/io/flume/FlumeConnector.java"},"Java class")))),(0,n.kt)("h3",{id:"twitter-firehose"},"Twitter firehose"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-twitter-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/twitter/src/main/java/org/apache/pulsar/io/twitter/TwitterFireHose.java"},"Java class")))),(0,n.kt)("h3",{id:"kafka"},"Kafka"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-kafka-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-kafka-source#usage"},"Example"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java"},"Java class")))),(0,n.kt)("h3",{id:"kinesis"},"Kinesis"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-kinesis-source#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/KinesisSource.java"},"Java class"))),(0,n.kt)("h3",{id:"netty"},"Netty"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-netty-source#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-netty-source#tcp"},"Example of TCP"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-netty-source#http"},"Example of HTTP"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/netty/src/main/java/org/apache/pulsar/io/netty/NettySource.java"},"Java class")))),(0,n.kt)("h3",{id:"nsq"},"NSQ"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-nsq-source#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/nsq/src/main/java/org/apache/pulsar/io/nsq/NSQSource.java"},"Java class"))),(0,n.kt)("h3",{id:"rabbitmq"},"RabbitMQ"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-rabbitmq-source#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/io/rabbitmq/RabbitMQSource.java"},"Java class"))),(0,n.kt)("h2",{id:"sink-connector"},"Sink connector"),(0,n.kt)("p",null,"Pulsar has various sink connectors, which are sorted alphabetically as below."),(0,n.kt)("h3",{id:"aerospike"},"Aerospike"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"/docs/next/io-aerospike-sink#configuration"},"Configuration"))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/aerospike/src/main/java/org/apache/pulsar/io/aerospike/AerospikeStringSink.java"},"Java class")))),(0,n.kt)("h3",{id:"cassandra"},"Cassandra"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-cassandra-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-cassandra-sink#usage"},"Example"),"  "),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/cassandra/src/main/java/org/apache/pulsar/io/cassandra/CassandraStringSink.java"},"Java class"))),(0,n.kt)("h3",{id:"elasticsearch"},"ElasticSearch"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-elasticsearch-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/elastic-search/src/main/java/org/apache/pulsar/io/elasticsearch/ElasticSearchSink.java"},"Java class"))),(0,n.kt)("h3",{id:"flume-1"},"Flume"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-flume-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/java/org/apache/pulsar/io/flume/sink/StringSink.java"},"Java class"))),(0,n.kt)("h3",{id:"hbase"},"HBase"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-hbase-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/hbase/src/main/java/org/apache/pulsar/io/hbase/HbaseAbstractConfig.java"},"Java class"))),(0,n.kt)("h3",{id:"hdfs2"},"HDFS2"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-hdfs2-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/hdfs2/src/main/java/org/apache/pulsar/io/hdfs2/AbstractHdfsConnector.java"},"Java class"))),(0,n.kt)("h3",{id:"hdfs3"},"HDFS3"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-hdfs3-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/hdfs3/src/main/java/org/apache/pulsar/io/hdfs3/AbstractHdfsConnector.java"},"Java class"))),(0,n.kt)("h3",{id:"influxdb"},"InfluxDB"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-influxdb-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/influxdb/src/main/java/org/apache/pulsar/io/influxdb/InfluxDBGenericRecordSink.java"},"Java class"))),(0,n.kt)("h3",{id:"jdbc-clickhouse"},"JDBC ClickHouse"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#example-for-clickhouse"},"Example")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/clickhouse/src/main/java/org/apache/pulsar/io/jdbc/ClickHouseJdbcAutoSchemaSink.java"},"Java class"))),(0,n.kt)("h3",{id:"jdbc-mariadb"},"JDBC MariaDB"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#example-for-mariadb"},"Example")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/mariadb/src/main/java/org/apache/pulsar/io/jdbc/MariadbJdbcAutoSchemaSink.java"},"Java class"))),(0,n.kt)("h3",{id:"jdbc-postgresql"},"JDBC PostgreSQL"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#example-for-postgresql"},"Example")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/postgres/src/main/java/org/apache/pulsar/io/jdbc/PostgresJdbcAutoSchemaSink.java"},"Java class"))),(0,n.kt)("h3",{id:"jdbc-sqlite"},"JDBC SQLite"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-jdbc-sink#example-for-sqlite"},"Example")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/sqlite/src/main/java/org/apache/pulsar/io/jdbc/SqliteJdbcAutoSchemaSink.java"},"Java class"))),(0,n.kt)("h3",{id:"kafka-1"},"Kafka"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-kafka-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSink.java"},"Java class"))),(0,n.kt)("h3",{id:"kinesis-1"},"Kinesis"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-kinesis-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/KinesisSink.java"},"Java class"))),(0,n.kt)("h3",{id:"mongodb"},"MongoDB"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-mongo-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/mongo/src/main/java/org/apache/pulsar/io/mongodb/MongoSink.java"},"Java class"))),(0,n.kt)("h3",{id:"rabbitmq-1"},"RabbitMQ"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-rabbitmq-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/io/rabbitmq/RabbitMQSink.java"},"Java class"))),(0,n.kt)("h3",{id:"redis"},"Redis"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-redis-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/redis/src/main/java/org/apache/pulsar/io/redis/RedisAbstractConfig.java"},"Java class"))),(0,n.kt)("h3",{id:"solr"},"Solr"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/docs/next/io-solr-sink#configuration"},"Configuration")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/pulsar-io/solr/src/main/java/org/apache/pulsar/io/solr/SolrSinkConfig.java"},"Java class"))))}m.isMDXComponent=!0}}]);
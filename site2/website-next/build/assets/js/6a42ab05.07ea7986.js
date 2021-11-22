"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[63532],{3905:function(e,n,t){t.d(n,{Zo:function(){return p},kt:function(){return d}});var a=t(67294);function o(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function r(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);n&&(a=a.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,a)}return t}function i(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?r(Object(t),!0).forEach((function(n){o(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):r(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function s(e,n){if(null==e)return{};var t,a,o=function(e,n){if(null==e)return{};var t,a,o={},r=Object.keys(e);for(a=0;a<r.length;a++)t=r[a],n.indexOf(t)>=0||(o[t]=e[t]);return o}(e,n);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)t=r[a],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var l=a.createContext({}),c=function(e){var n=a.useContext(l),t=n;return e&&(t="function"==typeof e?e(n):i(i({},n),e)),t},p=function(e){var n=c(e.components);return a.createElement(l.Provider,{value:n},e.children)},m={inlineCode:"code",wrapper:function(e){var n=e.children;return a.createElement(a.Fragment,{},n)}},u=a.forwardRef((function(e,n){var t=e.components,o=e.mdxType,r=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=c(t),d=o,g=u["".concat(l,".").concat(d)]||u[d]||m[d]||r;return t?a.createElement(g,i(i({ref:n},p),{},{components:t})):a.createElement(g,i({ref:n},p))}));function d(e,n){var t=arguments,o=n&&n.mdxType;if("string"==typeof e||o){var r=t.length,i=new Array(r);i[0]=u;var s={};for(var l in n)hasOwnProperty.call(n,l)&&(s[l]=n[l]);s.originalType=e,s.mdxType="string"==typeof e?e:o,i[1]=s;for(var c=2;c<r;c++)i[c]=t[c];return a.createElement.apply(null,i)}return a.createElement.apply(null,t)}u.displayName="MDXCreateElement"},77966:function(e,n,t){t.r(n),t.d(n,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return c},toc:function(){return p},default:function(){return u}});var a=t(87462),o=t(63366),r=(t(67294),t(3905)),i=["components"],s={id:"io-debug",title:"How to debug Pulsar connectors",sidebar_label:"Debug",original_id:"io-debug"},l=void 0,c={unversionedId:"io-debug",id:"version-2.8.0/io-debug",isDocsHomePage:!1,title:"How to debug Pulsar connectors",description:"This guide explains how to debug connectors in localrun or cluster mode and gives a debugging checklist.",source:"@site/versioned_docs/version-2.8.0/io-debug.md",sourceDirName:".",slug:"/io-debug",permalink:"/docs/io-debug",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.8.0/io-debug.md",tags:[],version:"2.8.0",frontMatter:{id:"io-debug",title:"How to debug Pulsar connectors",sidebar_label:"Debug",original_id:"io-debug"},sidebar:"version-2.8.0/docsSidebar",previous:{title:"Use",permalink:"/docs/io-use"},next:{title:"Built-in connector",permalink:"/docs/io-connectors"}},p=[{value:"Debug in localrun mode",id:"debug-in-localrun-mode",children:[{value:"Use connector log",id:"use-connector-log",children:[]}]},{value:"Debug in cluster mode",id:"debug-in-cluster-mode",children:[{value:"Use connector log",id:"use-connector-log-1",children:[]},{value:"Use admin CLI",id:"use-admin-cli",children:[]},{value:"<code>get</code>",id:"get",children:[]},{value:"<code>status</code>",id:"status",children:[]},{value:"<code>topics stats</code>",id:"topics-stats",children:[]}]},{value:"Checklist",id:"checklist",children:[]}],m={toc:p};function u(e){var n=e.components,t=(0,o.Z)(e,i);return(0,r.kt)("wrapper",(0,a.Z)({},m,t,{components:n,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"This guide explains how to debug connectors in localrun or cluster mode and gives a debugging checklist.\nTo better demonstrate how to debug Pulsar connectors, here takes a Mongo sink connector as an example.   "),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Deploy a Mongo sink environment")),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"Start a Mongo service."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\ndocker pull mongo:4\ndocker run -d -p 27017:27017 --name pulsar-mongo -v $PWD/data:/data/db mongo:4\n\n"))),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"Create a DB and a collection."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\ndocker exec -it pulsar-mongo /bin/bash\nmongo\n> use pulsar\n> db.createCollection('messages')\n> exit\n\n"))),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"Start Pulsar standalone."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\ndocker pull apachepulsar/pulsar:2.4.0\ndocker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --link pulsar-mongo --name pulsar-mongo-standalone apachepulsar/pulsar:2.4.0 bin/pulsar standalone\n\n"))),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"Configure the Mongo sink with the ",(0,r.kt)("inlineCode",{parentName:"p"},"mongo-sink-config.yaml")," file."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},'\nconfigs:\nmongoUri: "mongodb://pulsar-mongo:27017"\ndatabase: "pulsar"\ncollection: "messages"\nbatchSize: 2\nbatchTimeMs: 500\n\n')),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\ndocker cp mongo-sink-config.yaml pulsar-mongo-standalone:/pulsar/\n\n"))),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"Download the Mongo sink nar package."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\ndocker exec -it pulsar-mongo-standalone /bin/bash\ncurl -O http://apache.01link.hk/pulsar/pulsar-2.4.0/connectors/pulsar-io-mongo-2.4.0.nar\n\n")))),(0,r.kt)("h2",{id:"debug-in-localrun-mode"},"Debug in localrun mode"),(0,r.kt)("p",null,"Start the Mongo sink in localrun mode using the ",(0,r.kt)("inlineCode",{parentName:"p"},"localrun")," command."),(0,r.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"For more information about the ",(0,r.kt)("inlineCode",{parentName:"p"},"localrun")," command, see ",(0,r.kt)("a",{parentName:"p",href:"/docs/reference-connector-admin/#localrun-1"},(0,r.kt)("inlineCode",{parentName:"a"},"localrun")),"."))),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\n./bin/pulsar-admin sinks localrun \\\n--archive pulsar-io-mongo-2.4.0.nar \\ \n--tenant public --namespace default \\\n--inputs test-mongo \\\n--name pulsar-mongo-sink \\\n--sink-config-file mongo-sink-config.yaml \\\n--parallelism 1\n\n")),(0,r.kt)("h3",{id:"use-connector-log"},"Use connector log"),(0,r.kt)("p",null,"Use one of the following methods to get a connector log in localrun mode:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"After executing the ",(0,r.kt)("inlineCode",{parentName:"p"},"localrun")," command, the ",(0,r.kt)("strong",{parentName:"p"},"log is automatically printed on the console"),".")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"The log is located at:"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\nlogs/functions/tenant/namespace/function-name/function-name-instance-id.log\n\n")),(0,r.kt)("p",{parentName:"li"},(0,r.kt)("strong",{parentName:"p"},"Example")),(0,r.kt)("p",{parentName:"li"},"The path of the Mongo sink connector is:"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\nlogs/functions/public/default/pulsar-mongo-sink/pulsar-mongo-sink-0.log\n\n")))),(0,r.kt)("p",null,"To clearly explain the log information, here breaks down the large block of information into small blocks and add descriptions for each block."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"This piece of log information shows the storage path of the nar package after decompression."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"\n08:21:54.132 [main] INFO  org.apache.pulsar.common.nar.NarClassLoader - Created class loader with paths: [file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/, file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/META-INF/bundled-dependencies/,\n\n")),(0,r.kt)("div",{parentName:"li",className:"admonition admonition-tip alert alert--success"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"If ",(0,r.kt)("inlineCode",{parentName:"p"},"class cannot be found")," exception is thrown, check whether the nar file is decompressed in the folder ",(0,r.kt)("inlineCode",{parentName:"p"},"file:/tmp/pulsar-nar/pulsar-io-mongo-2.4.0.nar-unpacked/META-INF/bundled-dependencies/")," or not.")))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"This piece of log information illustrates the basic information about the Mongo sink connector, such as tenant, namespace, name, parallelism, resources, and so on, which can be used to ",(0,r.kt)("strong",{parentName:"p"},"check whether the Mongo sink connector is configured correctly or not"),"."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},'\n08:21:55.390 [main] INFO  org.apache.pulsar.functions.runtime.ThreadRuntime - ThreadContainer starting function with instance config InstanceConfig(instanceId=0, functionId=853d60a1-0c48-44d5-9a5c-6917386476b2, functionVersion=c2ce1458-b69e-4175-88c0-a0a856a2be8c, functionDetails=tenant: "public"\nnamespace: "default"\nname: "pulsar-mongo-sink"\nclassName: "org.apache.pulsar.functions.api.utils.IdentityFunction"\nautoAck: true\nparallelism: 1\nsource {\ntypeClassName: "[B"\ninputSpecs {\n    key: "test-mongo"\n    value {\n    }\n}\ncleanupSubscription: true\n}\nsink {\nclassName: "org.apache.pulsar.io.mongodb.MongoSink"\nconfigs: "{\\"mongoUri\\":\\"mongodb://pulsar-mongo:27017\\",\\"database\\":\\"pulsar\\",\\"collection\\":\\"messages\\",\\"batchSize\\":2,\\"batchTimeMs\\":500}"\ntypeClassName: "[B"\n}\nresources {\ncpu: 1.0\nram: 1073741824\ndisk: 10737418240\n}\ncomponentType: SINK\n, maxBufferedTuples=1024, functionAuthenticationSpec=null, port=38459, clusterName=local)\n\n'))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"This piece of log information demonstrates the status of the connections to Mongo and configuration information."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\n08:21:56.231 [cluster-ClusterId{value='5d6396a3c9e77c0569ff00eb', description='null'}-pulsar-mongo:27017] INFO  org.mongodb.driver.connection - Opened connection [connectionId{localValue:1, serverValue:8}] to pulsar-mongo:27017\n08:21:56.326 [cluster-ClusterId{value='5d6396a3c9e77c0569ff00eb', description='null'}-pulsar-mongo:27017] INFO  org.mongodb.driver.cluster - Monitor thread successfully connected to server with description ServerDescription{address=pulsar-mongo:27017, type=STANDALONE, state=CONNECTED, ok=true, version=ServerVersion{versionList=[4, 2, 0]}, minWireVersion=0, maxWireVersion=8, maxDocumentSize=16777216, logicalSessionTimeoutMinutes=30, roundTripTimeNanos=89058800}\n\n"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"This piece of log information explains the configuration of consumers and clients, including the topic name, subscription name, subscription type, and so on."),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-bash"},'\n08:21:56.719 [pulsar-client-io-1-1] INFO  org.apache.pulsar.client.impl.ConsumerStatsRecorderImpl - Starting Pulsar consumer status recorder with config: {\n"topicNames" : [ "test-mongo" ],\n"topicsPattern" : null,\n"subscriptionName" : "public/default/pulsar-mongo-sink",\n"subscriptionType" : "Shared",\n"receiverQueueSize" : 1000,\n"acknowledgementsGroupTimeMicros" : 100000,\n"negativeAckRedeliveryDelayMicros" : 60000000,\n"maxTotalReceiverQueueSizeAcrossPartitions" : 50000,\n"consumerName" : null,\n"ackTimeoutMillis" : 0,\n"tickDurationMillis" : 1000,\n"priorityLevel" : 0,\n"cryptoFailureAction" : "CONSUME",\n"properties" : {\n    "application" : "pulsar-sink",\n    "id" : "public/default/pulsar-mongo-sink",\n    "instance_id" : "0"\n},\n"readCompacted" : false,\n"subscriptionInitialPosition" : "Latest",\n"patternAutoDiscoveryPeriod" : 1,\n"regexSubscriptionMode" : "PersistentOnly",\n"deadLetterPolicy" : null,\n"autoUpdatePartitions" : true,\n"replicateSubscriptionState" : false,\n"resetIncludeHead" : false\n}\n08:21:56.726 [pulsar-client-io-1-1] INFO  org.apache.pulsar.client.impl.ConsumerStatsRecorderImpl - Pulsar client config: {\n"serviceUrl" : "pulsar://localhost:6650",\n"authPluginClassName" : null,\n"authParams" : null,\n"operationTimeoutMs" : 30000,\n"statsIntervalSeconds" : 60,\n"numIoThreads" : 1,\n"numListenerThreads" : 1,\n"connectionsPerBroker" : 1,\n"useTcpNoDelay" : true,\n"useTls" : false,\n"tlsTrustCertsFilePath" : null,\n"tlsAllowInsecureConnection" : false,\n"tlsHostnameVerificationEnable" : false,\n"concurrentLookupRequest" : 5000,\n"maxLookupRequest" : 50000,\n"maxNumberOfRejectedRequestPerConnection" : 50,\n"keepAliveIntervalSeconds" : 30,\n"connectionTimeoutMs" : 10000,\n"requestTimeoutMs" : 60000,\n"defaultBackoffIntervalNanos" : 100000000,\n"maxBackoffIntervalNanos" : 30000000000\n}\n\n')))),(0,r.kt)("h2",{id:"debug-in-cluster-mode"},"Debug in cluster mode"),(0,r.kt)("p",null,"You can use the following methods to debug a connector in cluster mode:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#use-connector-log"},"Use connector log")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#use-admin-cli"},"Use admin CLI"))),(0,r.kt)("h3",{id:"use-connector-log-1"},"Use connector log"),(0,r.kt)("p",null,"In cluster mode, multiple connectors can run on a worker. To find the log path of a specified connector, use the ",(0,r.kt)("inlineCode",{parentName:"p"},"workerId")," to locate the connector log."),(0,r.kt)("h3",{id:"use-admin-cli"},"Use admin CLI"),(0,r.kt)("p",null,"Pulsar admin CLI helps you debug Pulsar connectors with the following subcommands:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#get"},(0,r.kt)("inlineCode",{parentName:"a"},"get"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#status"},(0,r.kt)("inlineCode",{parentName:"a"},"status"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#topics-stats"},(0,r.kt)("inlineCode",{parentName:"a"},"topics stats")),"  ")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Create a Mongo sink")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\n./bin/pulsar-admin sinks create \\\n--archive pulsar-io-mongo-2.4.0.nar \\\n--tenant public \\\n--namespace default \\\n--inputs test-mongo \\\n--name pulsar-mongo-sink \\\n--sink-config-file mongo-sink-config.yaml \\\n--parallelism 1\n\n")),(0,r.kt)("h3",{id:"get"},(0,r.kt)("inlineCode",{parentName:"h3"},"get")),(0,r.kt)("p",null,"Use the ",(0,r.kt)("inlineCode",{parentName:"p"},"get")," command to get the basic information about the Mongo sink connector, such as tenant, namespace, name, parallelism, and so on."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},'\n./bin/pulsar-admin sinks get --tenant public --namespace default  --name pulsar-mongo-sink\n{\n  "tenant": "public",\n  "namespace": "default",\n  "name": "pulsar-mongo-sink",\n  "className": "org.apache.pulsar.io.mongodb.MongoSink",\n  "inputSpecs": {\n    "test-mongo": {\n      "isRegexPattern": false\n    }\n  },\n  "configs": {\n    "mongoUri": "mongodb://pulsar-mongo:27017",\n    "database": "pulsar",\n    "collection": "messages",\n    "batchSize": 2.0,\n    "batchTimeMs": 500.0\n  },\n  "parallelism": 1,\n  "processingGuarantees": "ATLEAST_ONCE",\n  "retainOrdering": false,\n  "autoAck": true\n}\n\n')),(0,r.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"For more information about the ",(0,r.kt)("inlineCode",{parentName:"p"},"get")," command, see ",(0,r.kt)("a",{parentName:"p",href:"/docs/reference-connector-admin/#get-1"},(0,r.kt)("inlineCode",{parentName:"a"},"get")),"."))),(0,r.kt)("h3",{id:"status"},(0,r.kt)("inlineCode",{parentName:"h3"},"status")),(0,r.kt)("p",null,"Use the ",(0,r.kt)("inlineCode",{parentName:"p"},"status")," command to get the current status about the Mongo sink connector, such as the number of instance, the number of running instance, instanceId, workerId and so on."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},'\n./bin/pulsar-admin sinks status \n--tenant public \\\n--namespace default  \\\n--name pulsar-mongo-sink\n{\n"numInstances" : 1,\n"numRunning" : 1,\n"instances" : [ {\n    "instanceId" : 0,\n    "status" : {\n    "running" : true,\n    "error" : "",\n    "numRestarts" : 0,\n    "numReadFromPulsar" : 0,\n    "numSystemExceptions" : 0,\n    "latestSystemExceptions" : [ ],\n    "numSinkExceptions" : 0,\n    "latestSinkExceptions" : [ ],\n    "numWrittenToSink" : 0,\n    "lastReceivedTime" : 0,\n    "workerId" : "c-standalone-fw-5d202832fd18-8080"\n    }\n} ]\n}\n\n')),(0,r.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"For more information about the ",(0,r.kt)("inlineCode",{parentName:"p"},"status")," command, see ",(0,r.kt)("a",{parentName:"p",href:"/docs/reference-connector-admin/#stauts-1"},(0,r.kt)("inlineCode",{parentName:"a"},"status")),".\nIf there are multiple connectors running on a worker, ",(0,r.kt)("inlineCode",{parentName:"p"},"workerId")," can locate the worker on which the specified connector is running."))),(0,r.kt)("h3",{id:"topics-stats"},(0,r.kt)("inlineCode",{parentName:"h3"},"topics stats")),(0,r.kt)("p",null,"Use the ",(0,r.kt)("inlineCode",{parentName:"p"},"topics stats")," command to get the stats for a topic and its connected producer and consumer, such as whether the topic has received messages or not, whether there is a backlog of messages or not, the available permits and other key information. All rates are computed over a 1-minute window and are relative to the last completed 1-minute period."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},'\n./bin/pulsar-admin topics stats test-mongo\n{\n  "msgRateIn" : 0.0,\n  "msgThroughputIn" : 0.0,\n  "msgRateOut" : 0.0,\n  "msgThroughputOut" : 0.0,\n  "averageMsgSize" : 0.0,\n  "storageSize" : 1,\n  "publishers" : [ ],\n  "subscriptions" : {\n    "public/default/pulsar-mongo-sink" : {\n      "msgRateOut" : 0.0,\n      "msgThroughputOut" : 0.0,\n      "msgRateRedeliver" : 0.0,\n      "msgBacklog" : 0,\n      "blockedSubscriptionOnUnackedMsgs" : false,\n      "msgDelayed" : 0,\n      "unackedMessages" : 0,\n      "type" : "Shared",\n      "msgRateExpired" : 0.0,\n      "consumers" : [ {\n        "msgRateOut" : 0.0,\n        "msgThroughputOut" : 0.0,\n        "msgRateRedeliver" : 0.0,\n        "consumerName" : "dffdd",\n        "availablePermits" : 999,\n        "unackedMessages" : 0,\n        "blockedConsumerOnUnackedMsgs" : false,\n        "metadata" : {\n          "instance_id" : "0",\n          "application" : "pulsar-sink",\n          "id" : "public/default/pulsar-mongo-sink"\n        },\n        "connectedSince" : "2019-08-26T08:48:07.582Z",\n        "clientVersion" : "2.4.0",\n        "address" : "/172.17.0.3:57790"\n      } ],\n      "isReplicated" : false\n    }\n  },\n  "replication" : { },\n  "deduplicationStatus" : "Disabled"\n}\n\n')),(0,r.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"For more information about the ",(0,r.kt)("inlineCode",{parentName:"p"},"topic stats")," command, see ",(0,r.kt)("a",{parentName:"p",href:"http://pulsar.apache.org/docs/en/pulsar-admin/#stats-1"},(0,r.kt)("inlineCode",{parentName:"a"},"topic stats")),"."))),(0,r.kt)("h2",{id:"checklist"},"Checklist"),(0,r.kt)("p",null,"This checklist indicates the major areas to check when you debug connectors. It is a reminder of what to look for to ensure a thorough review and an evaluation tool to get the status of connectors. "),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Does Pulsar start successfully?")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Does the external service run normally?")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Is the nar package complete?")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Is the connector configuration file correct?")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"In localrun mode, run a connector and check the printed information (connector log) on the console.")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"In cluster mode\uff1a"),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Use the ",(0,r.kt)("inlineCode",{parentName:"p"},"get")," command to get the basic information.")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Use the ",(0,r.kt)("inlineCode",{parentName:"p"},"status")," command to get the current status.")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Use the ",(0,r.kt)("inlineCode",{parentName:"p"},"topics stats")," command to get the stats for a specified topic and its connected producers and consumers.")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Check the connector log.")))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Enter into the external system and verify the result."))))}u.isMDXComponent=!0}}]);
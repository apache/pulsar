"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[41410],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return m}});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var c=a.createContext({}),l=function(e){var t=a.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=l(e.components);return a.createElement(c.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,c=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),d=l(n),m=r,h=d["".concat(c,".").concat(m)]||d[m]||u[m]||i;return n?a.createElement(h,o(o({ref:t},p),{},{components:n})):a.createElement(h,o({ref:t},p))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=d;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var l=2;l<i;l++)o[l]=n[l];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},20036:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return c},metadata:function(){return l},toc:function(){return p},default:function(){return d}});var a=n(87462),r=n(63366),i=(n(67294),n(3905)),o=["components"],s={id:"concepts-clients",title:"Pulsar Clients",sidebar_label:"Clients",original_id:"concepts-clients"},c=void 0,l={unversionedId:"concepts-clients",id:"version-2.6.2/concepts-clients",isDocsHomePage:!1,title:"Pulsar Clients",description:"Pulsar exposes a client API with language bindings for Java,  Go, Python, C++ and C#. The client API optimizes and encapsulates Pulsar's client-broker communication protocol and exposes a simple and intuitive API for use by applications.",source:"@site/versioned_docs/version-2.6.2/concepts-clients.md",sourceDirName:".",slug:"/concepts-clients",permalink:"/docs/2.6.2/concepts-clients",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.2/concepts-clients.md",tags:[],version:"2.6.2",frontMatter:{id:"concepts-clients",title:"Pulsar Clients",sidebar_label:"Clients",original_id:"concepts-clients"},sidebar:"version-2.6.2/docsSidebar",previous:{title:"Architecture",permalink:"/docs/2.6.2/concepts-architecture-overview"},next:{title:"Geo Replication",permalink:"/docs/2.6.2/concepts-replication"}},p=[{value:"Client setup phase",id:"client-setup-phase",children:[]},{value:"Reader interface",id:"reader-interface",children:[]}],u={toc:p};function d(e){var t=e.components,s=(0,r.Z)(e,o);return(0,i.kt)("wrapper",(0,a.Z)({},u,s,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Pulsar exposes a client API with language bindings for ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/client-libraries-java"},"Java"),",  ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/client-libraries-go"},"Go"),", ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/client-libraries-python"},"Python"),", ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/client-libraries-cpp"},"C++")," and ",(0,i.kt)("a",{parentName:"p",href:"client-libraries-dotnet"},"C#"),". The client API optimizes and encapsulates Pulsar's client-broker communication protocol and exposes a simple and intuitive API for use by applications."),(0,i.kt)("p",null,"Under the hood, the current official Pulsar client libraries support transparent reconnection and/or connection failover to brokers, queuing of messages until acknowledged by the broker, and heuristics such as connection retries with backoff."),(0,i.kt)("blockquote",null,(0,i.kt)("h4",{parentName:"blockquote",id:"custom-client-libraries"},"Custom client libraries"),(0,i.kt)("p",{parentName:"blockquote"},"If you'd like to create your own client library, we recommend consulting the documentation on Pulsar's custom ",(0,i.kt)("a",{parentName:"p",href:"developing-binary-protocol"},"binary protocol"))),(0,i.kt)("h2",{id:"client-setup-phase"},"Client setup phase"),(0,i.kt)("p",null,"When an application wants to create a producer/consumer, the Pulsar client library will initiate a setup phase that is composed of two steps:"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},"The client will attempt to determine the owner of the topic by sending an HTTP lookup request to the broker. The request could reach one of the active brokers which, by looking at the (cached) zookeeper metadata will know who is serving the topic or, in case nobody is serving it, will try to assign it to the least loaded broker."),(0,i.kt)("li",{parentName:"ol"},"Once the client library has the broker address, it will create a TCP connection (or reuse an existing connection from the pool) and authenticate it. Within this connection, client and broker exchange binary commands from a custom protocol. At this point the client will send a command to create producer/consumer to the broker, which will comply after having validated the authorization policy.")),(0,i.kt)("p",null,"Whenever the TCP connection breaks, the client will immediately re-initiate this setup phase and will keep trying with exponential backoff to re-establish the producer or consumer until the operation succeeds."),(0,i.kt)("h2",{id:"reader-interface"},"Reader interface"),(0,i.kt)("p",null,'In Pulsar, the "standard" ',(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/concepts-messaging#consumers"},"consumer interface")," involves using consumers to listen on ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#topic"},"topics"),", process incoming messages, and finally acknowledge those messages when they've been processed.  Whenever a new subscription is created, it is initially positioned at the end of the topic (by default), and consumers associated with that subscription will begin reading with the first message created afterwards.  Whenever a consumer connects to a topic using a pre-existing subscription, it begins reading from the earliest message un-acked within that subscription.  In summary, with the consumer interface, subscription cursors are automatically managed by Pulsar in response to ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/concepts-messaging#acknowledgement"},"message acknowledgements"),"."),(0,i.kt)("p",null,"The ",(0,i.kt)("strong",{parentName:"p"},"reader interface")," for Pulsar enables applications to manually manage cursors. When you use a reader to connect to a topic---rather than a consumer---you need to specify ",(0,i.kt)("em",{parentName:"p"},"which")," message the reader begins reading from when it connects to a topic. When connecting to a topic, the reader interface enables you to begin with:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("strong",{parentName:"li"},"earliest")," available message in the topic"),(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("strong",{parentName:"li"},"latest")," available message in the topic"),(0,i.kt)("li",{parentName:"ul"},'Some other message between the earliest and the latest. If you select this option, you\'ll need to explicitly provide a message ID. Your application will be responsible for "knowing" this message ID in advance, perhaps fetching it from a persistent data store or cache.')),(0,i.kt)("p",null,'The reader interface is helpful for use cases like using Pulsar to provide effectively-once processing semantics for a stream processing system. For this use case, it\'s essential that the stream processing system be able to "rewind" topics to a specific message and begin reading there. The reader interface provides Pulsar clients with the low-level abstraction necessary to "manually position" themselves within a topic.'),(0,i.kt)("p",null,"Internally, the reader interface is implemented as a consumer using an exclusive, non-durable subscription to the topic with a randomly-allocated name."),(0,i.kt)("p",null,"[ ",(0,i.kt)("strong",{parentName:"p"},"IMPORTANT")," ]"),(0,i.kt)("p",null,"Unlike subscription/consumer, readers are non-durable in nature and will not prevent data in a topic from being deleted, thus it is ",(0,i.kt)("strong",{parentName:"p"},(0,i.kt)("em",{parentName:"strong"},"strongly"))," advised that ",(0,i.kt)("a",{parentName:"p",href:"cookbooks-retention-expiry"},"data retention")," be configured.   If data retention for a topic is not configured for an adequate amount of time, messages that the reader has not yet read might be deleted .  This will cause readers to essentially skip messages.  Configuring the data retention for a topic guarantees the reader with have a certain duration to read a message."),(0,i.kt)("p",null,'Please also note that a reader can have a "backlog", but the metric is just to allow users to know how behind the reader is and is not considered for any backlog quota calculations. '),(0,i.kt)("p",null,(0,i.kt)("img",{alt:"The Pulsar consumer and reader interfaces",src:n(64223).Z})),(0,i.kt)("blockquote",null,(0,i.kt)("h3",{parentName:"blockquote",id:"non-partitioned-topics-only"},"Non-partitioned topics only"),(0,i.kt)("p",{parentName:"blockquote"},"The reader interface for Pulsar cannot currently be used with ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/concepts-messaging#partitioned-topics"},"partitioned topics"),".")),(0,i.kt)("p",null,"Here's a Java example that begins reading from the earliest available message on a topic:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'\nimport org.apache.pulsar.client.api.Message;\nimport org.apache.pulsar.client.api.MessageId;\nimport org.apache.pulsar.client.api.Reader;\n\n// Create a reader on a topic and for a specific message (and onward)\nReader<byte[]> reader = pulsarClient.newReader()\n    .topic("reader-api-test")\n    .startMessageId(MessageId.earliest)\n    .create();\n\nwhile (true) {\n    Message message = reader.readNext();\n\n    // Process the message\n}\n\n')),(0,i.kt)("p",null,"To create a reader that will read from the latest available message:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nReader<byte[]> reader = pulsarClient.newReader()\n    .topic(topic)\n    .startMessageId(MessageId.latest)\n    .create();\n\n")),(0,i.kt)("p",null,"To create a reader that will read from some message between earliest and latest:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nbyte[] msgIdBytes = // Some byte array\nMessageId id = MessageId.fromByteArray(msgIdBytes);\nReader<byte[]> reader = pulsarClient.newReader()\n    .topic(topic)\n    .startMessageId(id)\n    .create();\n\n")))}d.isMDXComponent=!0},64223:function(e,t,n){t.Z=n.p+"assets/images/pulsar-reader-consumer-interfaces-4c838d3fa1b811f2c074087d44b991ea.png"}}]);
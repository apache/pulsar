"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[57785],{3905:function(e,t,n){n.d(t,{Zo:function(){return c},kt:function(){return u}});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),p=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},c=function(e){var t=p(e.components);return a.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),m=p(n),u=r,h=m["".concat(l,".").concat(u)]||m[u]||d[u]||i;return n?a.createElement(h,o(o({ref:t},c),{},{components:n})):a.createElement(h,o({ref:t},c))}));function u(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var p=2;p<i;p++)o[p]=n[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},86125:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return p},toc:function(){return c},default:function(){return m}});var a=n(87462),r=n(63366),i=(n(67294),n(3905)),o=["components"],s={id:"admin-api-non-partitioned-topics",title:"Managing non-partitioned topics",sidebar_label:"Non-Partitioned topics",original_id:"admin-api-non-partitioned-topics"},l=void 0,p={unversionedId:"admin-api-non-partitioned-topics",id:"version-2.6.2/admin-api-non-partitioned-topics",isDocsHomePage:!1,title:"Managing non-partitioned topics",description:"You can use Pulsar's admin API to create and manage non-partitioned topics.",source:"@site/versioned_docs/version-2.6.2/admin-api-non-partitioned-topics.md",sourceDirName:".",slug:"/admin-api-non-partitioned-topics",permalink:"/docs/2.6.2/admin-api-non-partitioned-topics",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.2/admin-api-non-partitioned-topics.md",tags:[],version:"2.6.2",frontMatter:{id:"admin-api-non-partitioned-topics",title:"Managing non-partitioned topics",sidebar_label:"Non-Partitioned topics",original_id:"admin-api-non-partitioned-topics"},sidebar:"version-2.6.2/docsSidebar",previous:{title:"Partitioned topics",permalink:"/docs/2.6.2/admin-api-partitioned-topics"},next:{title:"Functions",permalink:"/docs/2.6.2/admin-api-functions"}},c=[{value:"Non-Partitioned topics resources",id:"non-partitioned-topics-resources",children:[{value:"Create",id:"create",children:[]},{value:"Delete",id:"delete",children:[]},{value:"List",id:"list",children:[]},{value:"Stats",id:"stats",children:[]}]}],d={toc:c};function m(e){var t=e.components,n=(0,r.Z)(e,o);return(0,i.kt)("wrapper",(0,a.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"You can use Pulsar's ",(0,i.kt)("a",{parentName:"p",href:"admin-api-overview"},"admin API")," to create and manage non-partitioned topics."),(0,i.kt)("p",null,"In all of the instructions and commands below, the topic name structure is:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\npersistent://tenant/namespace/topic\n\n")),(0,i.kt)("h2",{id:"non-partitioned-topics-resources"},"Non-Partitioned topics resources"),(0,i.kt)("h3",{id:"create"},"Create"),(0,i.kt)("p",null,"Non-partitioned topics in Pulsar must be explicitly created. When creating a new non-partitioned topic you\nneed to provide a name for the topic."),(0,i.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"By default, after 60 seconds of creation, topics are considered inactive and deleted automatically to prevent from generating trash data.\nTo disable this feature, set ",(0,i.kt)("inlineCode",{parentName:"p"},"brokerDeleteInactiveTopicsEnabled"),"  to ",(0,i.kt)("inlineCode",{parentName:"p"},"false"),".\nTo change the frequency of checking inactive topics, set ",(0,i.kt)("inlineCode",{parentName:"p"},"brokerDeleteInactiveTopicsFrequencySeconds")," to your desired value.\nFor more information about these two parameters, see ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-configuration#broker"},"here"),"."))),(0,i.kt)("h4",{id:"pulsar-admin"},"pulsar-admin"),(0,i.kt)("p",null,"You can create non-partitioned topics using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#create-3"},(0,i.kt)("inlineCode",{parentName:"a"},"create")),"\ncommand and specifying the topic name as an argument.\nHere's an example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ bin/pulsar-admin topics create \\\n  persistent://my-tenant/my-namespace/my-topic\n\n")),(0,i.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"It's only allowed to create non partitioned topic of name contains suffix '-partition-' followed by numeric value like\n'xyz-topic-partition-10', if there's already a partitioned topic with same name, in this case 'xyz-topic', and has\nnumber of partition larger then that numeric value in this case 11(partition index is start from 0). Else creation of such topic will fail."))),(0,i.kt)("h4",{id:"rest-api"},"REST API"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/createNonPartitionedTopic?version=2.6.2&apiVersion=v2"},"PUT /admin/v2/:schema/:tenant/:namespace/:topic")),(0,i.kt)("h4",{id:"java"},"Java"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'\nString topicName = "persistent://my-tenant/my-namespace/my-topic";\nadmin.topics().createNonPartitionedTopic(topicName);\n\n')),(0,i.kt)("h3",{id:"delete"},"Delete"),(0,i.kt)("h4",{id:"pulsar-admin-1"},"pulsar-admin"),(0,i.kt)("p",null,"Non-partitioned topics can be deleted using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#delete-4"},(0,i.kt)("inlineCode",{parentName:"a"},"delete"))," command, specifying the topic by name:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ bin/pulsar-admin topics delete \\\n  persistent://my-tenant/my-namespace/my-topic\n\n")),(0,i.kt)("h4",{id:"rest-api-1"},"REST API"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/deleteTopic?version=2.6.2&apiVersion=v2"},"DELETE /admin/v2/:schema/:tenant/:namespace/:topic")),(0,i.kt)("h4",{id:"java-1"},"Java"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.topics().delete(persistentTopic);\n\n")),(0,i.kt)("h3",{id:"list"},"List"),(0,i.kt)("p",null,"It provides a list of topics existing under a given namespace.  "),(0,i.kt)("h4",{id:"pulsar-admin-2"},"pulsar-admin"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin topics list tenant/namespace\npersistent://tenant/namespace/topic1\npersistent://tenant/namespace/topic2\n\n")),(0,i.kt)("h4",{id:"rest-api-2"},"REST API"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getList?version=2.6.2&apiVersion=v2"},"GET /admin/v2/:schema/:tenant/:namespace")),(0,i.kt)("h4",{id:"java-2"},"Java"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.topics().getList(namespace);\n\n")),(0,i.kt)("h3",{id:"stats"},"Stats"),(0,i.kt)("p",null,"It shows current statistics of a given topic. Here's an example payload:"),(0,i.kt)("p",null,"The following stats are available:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Stat"),(0,i.kt)("th",{parentName:"tr",align:null},"Description"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgRateIn"),(0,i.kt)("td",{parentName:"tr",align:null},"The sum of all local and replication publishers\u2019 publish rates in messages per second")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgThroughputIn"),(0,i.kt)("td",{parentName:"tr",align:null},"Same as msgRateIn but in bytes per second instead of messages per second")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgRateOut"),(0,i.kt)("td",{parentName:"tr",align:null},"The sum of all local and replication consumers\u2019 dispatch rates in messages per second")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgThroughputOut"),(0,i.kt)("td",{parentName:"tr",align:null},"Same as msgRateOut but in bytes per second instead of messages per second")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"averageMsgSize"),(0,i.kt)("td",{parentName:"tr",align:null},"Average message size, in bytes, from this publisher within the last interval")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"storageSize"),(0,i.kt)("td",{parentName:"tr",align:null},"The sum of the ledgers\u2019 storage size for this topic")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"publishers"),(0,i.kt)("td",{parentName:"tr",align:null},"The list of all local publishers into the topic. There can be anywhere from zero to thousands.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"producerId"),(0,i.kt)("td",{parentName:"tr",align:null},"Internal identifier for this producer on this topic")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"producerName"),(0,i.kt)("td",{parentName:"tr",align:null},"Internal identifier for this producer, generated by the client library")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"address"),(0,i.kt)("td",{parentName:"tr",align:null},"IP address and source port for the connection of this producer")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"connectedSince"),(0,i.kt)("td",{parentName:"tr",align:null},"Timestamp this producer was created or last reconnected")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"subscriptions"),(0,i.kt)("td",{parentName:"tr",align:null},"The list of all local subscriptions to the topic")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"my-subscription"),(0,i.kt)("td",{parentName:"tr",align:null},"The name of this subscription (client defined)")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgBacklog"),(0,i.kt)("td",{parentName:"tr",align:null},"The count of messages in backlog for this subscription")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgBacklogNoDelayed"),(0,i.kt)("td",{parentName:"tr",align:null},"The count of messages in backlog without delayed messages for this subscription")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"type"),(0,i.kt)("td",{parentName:"tr",align:null},"This subscription type")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgRateExpired"),(0,i.kt)("td",{parentName:"tr",align:null},"The rate at which messages were discarded instead of dispatched from this subscription due to TTL")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"consumers"),(0,i.kt)("td",{parentName:"tr",align:null},"The list of connected consumers for this subscription")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"consumerName"),(0,i.kt)("td",{parentName:"tr",align:null},"Internal identifier for this consumer, generated by the client library")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"availablePermits"),(0,i.kt)("td",{parentName:"tr",align:null},"The number of messages this consumer has space for in the client library\u2019s listen queue. A value of 0 means the client library\u2019s queue is full and receive() isn\u2019t being called. A nonzero value means this consumer is ready to be dispatched messages.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"replication"),(0,i.kt)("td",{parentName:"tr",align:null},"This section gives the stats for cross-colo replication of this topic")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"replicationBacklog"),(0,i.kt)("td",{parentName:"tr",align:null},"The outbound replication backlog in messages")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"connected"),(0,i.kt)("td",{parentName:"tr",align:null},"Whether the outbound replicator is connected")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"replicationDelayInSeconds"),(0,i.kt)("td",{parentName:"tr",align:null},"How long the oldest message has been waiting to be sent through the connection, if connected is true")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"inboundConnection"),(0,i.kt)("td",{parentName:"tr",align:null},"The IP and port of the broker in the remote cluster\u2019s publisher connection to this broker")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"inboundConnectedSince"),(0,i.kt)("td",{parentName:"tr",align:null},"The TCP connection being used to publish messages to the remote cluster. If there are no local publishers connected, this connection is automatically closed after a minute.")))),(0,i.kt)("h4",{id:"pulsar-admin-3"},"pulsar-admin"),(0,i.kt)("p",null,"The stats for the topic and its connected producers and consumers can be fetched by using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#stats"},(0,i.kt)("inlineCode",{parentName:"a"},"stats"))," command, specifying the topic by name:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin topics stats \\\n  persistent://test-tenant/namespace/topic \\\n  --get-precise-backlog\n\n")),(0,i.kt)("h4",{id:"rest-api-3"},"REST API"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getStats?version=2.6.2&apiVersion=v2"},"GET /admin/v2/:schema/:tenant/:namespace/:topic/stats")),(0,i.kt)("h4",{id:"java-3"},"Java"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.topics().getStats(persistentTopic, false /* is precise backlog */);\n\n")))}m.isMDXComponent=!0}}]);
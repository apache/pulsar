"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[11276],{3905:function(t,e,n){n.d(e,{Zo:function(){return p},kt:function(){return c}});var a=n(67294);function r(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function i(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(t);e&&(a=a.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,a)}return n}function l(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?i(Object(n),!0).forEach((function(e){r(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}function s(t,e){if(null==t)return{};var n,a,r=function(t,e){if(null==t)return{};var n,a,r={},i=Object.keys(t);for(a=0;a<i.length;a++)n=i[a],e.indexOf(n)>=0||(r[n]=t[n]);return r}(t,e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(t);for(a=0;a<i.length;a++)n=i[a],e.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(t,n)&&(r[n]=t[n])}return r}var o=a.createContext({}),d=function(t){var e=a.useContext(o),n=e;return t&&(n="function"==typeof t?t(e):l(l({},e),t)),n},p=function(t){var e=d(t.components);return a.createElement(o.Provider,{value:e},t.children)},u={inlineCode:"code",wrapper:function(t){var e=t.children;return a.createElement(a.Fragment,{},e)}},m=a.forwardRef((function(t,e){var n=t.components,r=t.mdxType,i=t.originalType,o=t.parentName,p=s(t,["components","mdxType","originalType","parentName"]),m=d(n),c=r,g=m["".concat(o,".").concat(c)]||m[c]||u[c]||i;return n?a.createElement(g,l(l({ref:e},p),{},{components:n})):a.createElement(g,l({ref:e},p))}));function c(t,e){var n=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var i=n.length,l=new Array(i);l[0]=m;var s={};for(var o in e)hasOwnProperty.call(e,o)&&(s[o]=e[o]);s.originalType=t,s.mdxType="string"==typeof t?t:r,l[1]=s;for(var d=2;d<i;d++)l[d]=n[d];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},80528:function(t,e,n){n.r(e),n.d(e,{frontMatter:function(){return s},contentTitle:function(){return o},metadata:function(){return d},toc:function(){return p},default:function(){return m}});var a=n(87462),r=n(63366),i=(n(67294),n(3905)),l=["components"],s={id:"administration-stats",title:"Pulsar stats",sidebar_label:"Pulsar statistics",original_id:"administration-stats"},o=void 0,d={unversionedId:"administration-stats",id:"version-2.7.1/administration-stats",isDocsHomePage:!1,title:"Pulsar stats",description:"Partitioned topics",source:"@site/versioned_docs/version-2.7.1/administration-stats.md",sourceDirName:".",slug:"/administration-stats",permalink:"/docs/2.7.1/administration-stats",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.1/administration-stats.md",tags:[],version:"2.7.1",frontMatter:{id:"administration-stats",title:"Pulsar stats",sidebar_label:"Pulsar statistics",original_id:"administration-stats"},sidebar:"version-2.7.1/docsSidebar",previous:{title:"Pulsar Manager",permalink:"/docs/2.7.1/administration-pulsar-manager"},next:{title:"Load balance",permalink:"/docs/2.7.1/administration-load-balance"}},p=[{value:"Partitioned topics",id:"partitioned-topics",children:[]},{value:"Topics",id:"topics",children:[]}],u={toc:p};function m(t){var e=t.components,n=(0,r.Z)(t,l);return(0,i.kt)("wrapper",(0,a.Z)({},u,n,{components:e,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"partitioned-topics"},"Partitioned topics"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Stat"),(0,i.kt)("th",{parentName:"tr",align:null},"Description"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgRateIn"),(0,i.kt)("td",{parentName:"tr",align:null},"The sum of publish rates of all local and replication publishers in messages per second.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgThroughputIn"),(0,i.kt)("td",{parentName:"tr",align:null},"Same as msgRateIn but in bytes per second instead of messages per second.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgRateOut"),(0,i.kt)("td",{parentName:"tr",align:null},"The sum of dispatch rates of all local and replication consumers in messages per second.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgThroughputOut"),(0,i.kt)("td",{parentName:"tr",align:null},"Same as msgRateOut but in bytes per second instead of messages per second.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"averageMsgSize"),(0,i.kt)("td",{parentName:"tr",align:null},"Average message size, in bytes, from this publisher within the last interval.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"storageSize"),(0,i.kt)("td",{parentName:"tr",align:null},"The sum of storage size of the ledgers for this topic.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"publishers"),(0,i.kt)("td",{parentName:"tr",align:null},"The list of all local publishers into the topic. Publishers can be anywhere from zero to thousands.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"producerId"),(0,i.kt)("td",{parentName:"tr",align:null},"Internal identifier for this producer on this topic.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"producerName"),(0,i.kt)("td",{parentName:"tr",align:null},"Internal identifier for this producer, generated by the client library.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"address"),(0,i.kt)("td",{parentName:"tr",align:null},"IP address and source port for the connection of this producer.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"connectedSince"),(0,i.kt)("td",{parentName:"tr",align:null},"Timestamp this producer is created or last reconnected.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"subscriptions"),(0,i.kt)("td",{parentName:"tr",align:null},"The list of all local subscriptions to the topic.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"my-subscription"),(0,i.kt)("td",{parentName:"tr",align:null},"The name of this subscription (client defined).")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgBacklog"),(0,i.kt)("td",{parentName:"tr",align:null},"The count of messages in backlog for this subscription.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"type"),(0,i.kt)("td",{parentName:"tr",align:null},"This subscription type.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"msgRateExpired"),(0,i.kt)("td",{parentName:"tr",align:null},"The rate at which messages are discarded instead of dispatched from this subscription due to TTL.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"consumers"),(0,i.kt)("td",{parentName:"tr",align:null},"The list of connected consumers for this subscription.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"consumerName"),(0,i.kt)("td",{parentName:"tr",align:null},"Internal identifier for this consumer, generated by the client library.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"availablePermits"),(0,i.kt)("td",{parentName:"tr",align:null},"The number of messages this consumer has space for in the listen queue of client library. A value of 0 means the queue of client library is full and receive() is not being called. A nonzero value means this consumer is ready to be dispatched messages.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"replication"),(0,i.kt)("td",{parentName:"tr",align:null},"This section gives the stats for cross-colo replication of this topic.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"replicationBacklog"),(0,i.kt)("td",{parentName:"tr",align:null},"The outbound replication backlog in messages.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"connected"),(0,i.kt)("td",{parentName:"tr",align:null},"Whether the outbound replicator is connected.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"replicationDelayInSeconds"),(0,i.kt)("td",{parentName:"tr",align:null},"How long the oldest message has been waiting to be sent through the connection, if connected is true.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"inboundConnection"),(0,i.kt)("td",{parentName:"tr",align:null},"The IP and port of the broker in the publisher connection of remote cluster to this broker.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"inboundConnectedSince"),(0,i.kt)("td",{parentName:"tr",align:null},"The TCP connection being used to publish messages to the remote cluster. If no local publishers are connected, this connection is automatically closed after a minute.")))),(0,i.kt)("h2",{id:"topics"},"Topics"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Stat"),(0,i.kt)("th",{parentName:"tr",align:null},"Description"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"entriesAddedCounter"),(0,i.kt)("td",{parentName:"tr",align:null},"Messages published since this broker loads this topic.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"numberOfEntries"),(0,i.kt)("td",{parentName:"tr",align:null},"Total number of messages being tracked.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"totalSize"),(0,i.kt)("td",{parentName:"tr",align:null},"Total storage size in bytes of all messages.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"currentLedgerEntries"),(0,i.kt)("td",{parentName:"tr",align:null},"Count of messages written to the ledger currently open for writing.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"currentLedgerSize"),(0,i.kt)("td",{parentName:"tr",align:null},"Size in bytes of messages written to ledger currently open for writing.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"lastLedgerCreatedTimestamp"),(0,i.kt)("td",{parentName:"tr",align:null},"Time when last ledger is created.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"lastLedgerCreationFailureTimestamp"),(0,i.kt)("td",{parentName:"tr",align:null},"Time when last ledger is failed.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"waitingCursorsCount"),(0,i.kt)("td",{parentName:"tr",align:null},"How many cursors are caught up and waiting for a new message to be published.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"pendingAddEntriesCount"),(0,i.kt)("td",{parentName:"tr",align:null},"How many messages have (asynchronous) write requests you are waiting on completion.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"lastConfirmedEntry"),(0,i.kt)("td",{parentName:"tr",align:null},"The ledgerid:entryid of the last message successfully written. If the entryid is -1, then the ledger is opened or is being currently opened but has no entries written yet.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"state"),(0,i.kt)("td",{parentName:"tr",align:null},"The state of the cursor ledger. Open means you have a cursor ledger for saving updates of the markDeletePosition.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"ledgers"),(0,i.kt)("td",{parentName:"tr",align:null},"The ordered list of all ledgers for this topic holding its messages.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"cursors"),(0,i.kt)("td",{parentName:"tr",align:null},"The list of all cursors on this topic. Every subscription you saw in the topic stats has one.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"markDeletePosition"),(0,i.kt)("td",{parentName:"tr",align:null},"The ack position: the last message the subscriber acknowledges receiving.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"readPosition"),(0,i.kt)("td",{parentName:"tr",align:null},"The latest position of subscriber for reading message.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"waitingReadOp"),(0,i.kt)("td",{parentName:"tr",align:null},"This is true when the subscription reads the latest message that is published to the topic and waits on new messages to be published.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"pendingReadOps"),(0,i.kt)("td",{parentName:"tr",align:null},"The counter for how many outstanding read requests to the BookKeepers you have in progress.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"messagesConsumedCounter"),(0,i.kt)("td",{parentName:"tr",align:null},"Number of messages this cursor acks since this broker loads this topic.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"cursorLedger"),(0,i.kt)("td",{parentName:"tr",align:null},"The ledger used to persistently store the current markDeletePosition.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"cursorLedgerLastEntry"),(0,i.kt)("td",{parentName:"tr",align:null},"The last entryid used to persistently store the current markDeletePosition.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"individuallyDeletedMessages"),(0,i.kt)("td",{parentName:"tr",align:null},"If Acks are done out of order, shows the ranges of messages Acked between the markDeletePosition and the read-position.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"lastLedgerSwitchTimestamp"),(0,i.kt)("td",{parentName:"tr",align:null},"The last time the cursor ledger is rolled over.")))))}m.isMDXComponent=!0}}]);
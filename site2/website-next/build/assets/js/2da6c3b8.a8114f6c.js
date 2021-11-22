"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[11078],{3905:function(t,e,a){a.d(e,{Zo:function(){return d},kt:function(){return u}});var n=a(67294);function r(t,e,a){return e in t?Object.defineProperty(t,e,{value:a,enumerable:!0,configurable:!0,writable:!0}):t[e]=a,t}function o(t,e){var a=Object.keys(t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(t);e&&(n=n.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),a.push.apply(a,n)}return a}function i(t){for(var e=1;e<arguments.length;e++){var a=null!=arguments[e]?arguments[e]:{};e%2?o(Object(a),!0).forEach((function(e){r(t,e,a[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(a,e))}))}return t}function s(t,e){if(null==t)return{};var a,n,r=function(t,e){if(null==t)return{};var a,n,r={},o=Object.keys(t);for(n=0;n<o.length;n++)a=o[n],e.indexOf(a)>=0||(r[a]=t[a]);return r}(t,e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(t);for(n=0;n<o.length;n++)a=o[n],e.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(t,a)&&(r[a]=t[a])}return r}var l=n.createContext({}),c=function(t){var e=n.useContext(l),a=e;return t&&(a="function"==typeof t?t(e):i(i({},e),t)),a},d=function(t){var e=c(t.components);return n.createElement(l.Provider,{value:e},t.children)},p={inlineCode:"code",wrapper:function(t){var e=t.children;return n.createElement(n.Fragment,{},e)}},h=n.forwardRef((function(t,e){var a=t.components,r=t.mdxType,o=t.originalType,l=t.parentName,d=s(t,["components","mdxType","originalType","parentName"]),h=c(a),u=r,m=h["".concat(l,".").concat(u)]||h[u]||p[u]||o;return a?n.createElement(m,i(i({ref:e},d),{},{components:a})):n.createElement(m,i({ref:e},d))}));function u(t,e){var a=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var o=a.length,i=new Array(o);i[0]=h;var s={};for(var l in e)hasOwnProperty.call(e,l)&&(s[l]=e[l]);s.originalType=t,s.mdxType="string"==typeof t?t:r,i[1]=s;for(var c=2;c<o;c++)i[c]=a[c];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}h.displayName="MDXCreateElement"},88953:function(t,e,a){a.r(e),a.d(e,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return c},toc:function(){return d},default:function(){return h}});var n=a(87462),r=a(63366),o=(a(67294),a(3905)),i=["components"],s={id:"txn-how",title:"How transactions work?",sidebar_label:"How transactions work?"},l=void 0,c={unversionedId:"txn-how",id:"txn-how",isDocsHomePage:!1,title:"How transactions work?",description:"This section describes transaction components and how the components work together. For the complete design details, see PIP-31: Transactional Streaming.",source:"@site/docs/txn-how.md",sourceDirName:".",slug:"/txn-how",permalink:"/docs/next/txn-how",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/docs/txn-how.md",tags:[],version:"current",frontMatter:{id:"txn-how",title:"How transactions work?",sidebar_label:"How transactions work?"},sidebar:"docsSidebar",previous:{title:"What are transactions?",permalink:"/docs/next/txn-what"},next:{title:"How to use transactions?",permalink:"/docs/next/txn-use"}},d=[{value:"Key concept",id:"key-concept",children:[{value:"Transaction coordinator",id:"transaction-coordinator",children:[]},{value:"Transaction log",id:"transaction-log",children:[]},{value:"Transaction buffer",id:"transaction-buffer",children:[]},{value:"Transaction ID",id:"transaction-id",children:[]},{value:"Pending acknowledge state",id:"pending-acknowledge-state",children:[]}]},{value:"Data flow",id:"data-flow",children:[{value:"1. Begin a transaction",id:"1-begin-a-transaction",children:[]},{value:"2. Publish messages with a transaction",id:"2-publish-messages-with-a-transaction",children:[]},{value:"3. Acknowledge messages with a transaction",id:"3-acknowledge-messages-with-a-transaction",children:[]},{value:"4. End a transaction",id:"4-end-a-transaction",children:[]}]}],p={toc:d};function h(t){var e=t.components,s=(0,r.Z)(t,i);return(0,o.kt)("wrapper",(0,n.Z)({},p,s,{components:e,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"This section describes transaction components and how the components work together. For the complete design details, see ",(0,o.kt)("a",{parentName:"p",href:"https://docs.google.com/document/d/145VYp09JKTw9jAT-7yNyFU255FptB2_B2Fye100ZXDI/edit#heading=h.bm5ainqxosrx"},"PIP-31: Transactional Streaming"),"."),(0,o.kt)("h2",{id:"key-concept"},"Key concept"),(0,o.kt)("p",null,"It is important to know the following key concepts, which is a prerequisite for understanding how transactions work."),(0,o.kt)("h3",{id:"transaction-coordinator"},"Transaction coordinator"),(0,o.kt)("p",null,"The transaction coordinator (TC) is a module running inside a Pulsar broker. "),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"It maintains the entire life cycle of transactions and prevents a transaction from getting into an incorrect status. "),(0,o.kt)("li",{parentName:"ul"},"It handles transaction timeout, and ensures that the transaction is aborted after a transaction timeout.")),(0,o.kt)("h3",{id:"transaction-log"},"Transaction log"),(0,o.kt)("p",null,"All the transaction metadata persists in the transaction log. The transaction log is backed by a Pulsar topic. If the transaction coordinator crashes, it can restore the transaction metadata from the transaction log."),(0,o.kt)("p",null,"The transaction log stores the transaction status rather than actual messages in the transaction (the actual messages are stored in the actual topic partitions). "),(0,o.kt)("h3",{id:"transaction-buffer"},"Transaction buffer"),(0,o.kt)("p",null,"Messages produced to a topic partition within a transaction are stored in the transaction buffer (TB) of that topic partition. The messages in the transaction buffer are not visible to consumers until the transactions are committed. The messages in the transaction buffer are discarded when the transactions are aborted. "),(0,o.kt)("p",null,"Transaction buffer stores all ongoing and aborted transactions in memory. All messages are sent to the actual partitioned Pulsar topics.  After transactions are committed, the messages in the transaction buffer are materialized (visible) to consumers. When the transactions are aborted, the messages in the transaction buffer are discarded."),(0,o.kt)("h3",{id:"transaction-id"},"Transaction ID"),(0,o.kt)("p",null,"Transaction ID (TxnID) identifies a unique transaction in Pulsar. The transaction ID is 128-bit. The highest 16 bits are reserved for the ID of the transaction coordinator, and the remaining bits are used for monotonically increasing numbers in each transaction coordinator. It is easy to locate the transaction crash with the TxnID."),(0,o.kt)("h3",{id:"pending-acknowledge-state"},"Pending acknowledge state"),(0,o.kt)("p",null,"Pending acknowledge state maintains message acknowledgments within a transaction before a transaction completes. If a message is in the pending acknowledge state, the message cannot be acknowledged by other transactions until the message is removed from the pending acknowledge state."),(0,o.kt)("p",null,"The pending acknowledge state is persisted to the pending acknowledge log (cursor ledger). A new broker can restore the state from the pending acknowledge log to ensure the acknowledgement is not lost.    "),(0,o.kt)("h2",{id:"data-flow"},"Data flow"),(0,o.kt)("p",null,"At a high level, the data flow can be split into several steps:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Begin a transaction."),(0,o.kt)("li",{parentName:"ol"},"Publish messages with a transaction."),(0,o.kt)("li",{parentName:"ol"},"Acknowledge messages with a transaction."),(0,o.kt)("li",{parentName:"ol"},"End a transaction.")),(0,o.kt)("p",null,"To help you debug or tune the transaction for better performance, review the following diagrams and descriptions. "),(0,o.kt)("h3",{id:"1-begin-a-transaction"},"1. Begin a transaction"),(0,o.kt)("p",null,"Before introducing the transaction in Pulsar, a producer is created and then messages are sent to brokers and stored in data logs. "),(0,o.kt)("p",null,(0,o.kt)("img",{src:a(32805).Z})),(0,o.kt)("p",null,"Let\u2019s walk through the steps for ",(0,o.kt)("em",{parentName:"p"},"beginning a transaction"),"."),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Step"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"1.1"),(0,o.kt)("td",{parentName:"tr",align:null},"The first step is that the Pulsar client finds the transaction coordinator.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"1.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator allocates a transaction ID for the transaction. In the transaction log, the transaction is logged with its transaction ID and status (OPEN), which ensures the transaction status is persisted regardless of transaction coordinator crashes.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"1.3"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction log sends the result of persisting the transaction ID to the transaction coordinator.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"1.4"),(0,o.kt)("td",{parentName:"tr",align:null},"After the transaction status entry is logged, the transaction coordinator brings the transaction ID back to the Pulsar client.")))),(0,o.kt)("h3",{id:"2-publish-messages-with-a-transaction"},"2. Publish messages with a transaction"),(0,o.kt)("p",null,"In this stage, the Pulsar client enters a transaction loop, repeating the ",(0,o.kt)("inlineCode",{parentName:"p"},"consume-process-produce")," operation for all the messages that comprise the transaction. This is a long phase and is potentially composed of multiple produce and acknowledgement requests. "),(0,o.kt)("p",null,(0,o.kt)("img",{src:a(5014).Z})),(0,o.kt)("p",null,"Let\u2019s walk through the steps for ",(0,o.kt)("em",{parentName:"p"},"publishing messages with a transaction"),"."),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Step"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"2.1.1"),(0,o.kt)("td",{parentName:"tr",align:null},"Before the Pulsar client produces messages to a new topic partition, it sends a request to the transaction coordinator to add the partition to the transaction.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"2.1.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator logs the partition changes of the transaction into the transaction log for durability, which ensures the transaction coordinator knows all the partitions that a transaction is handling. The transaction coordinator can commit or abort changes on each partition at the end-partition phase.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"2.1.3"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction log sends the result of logging the new partition (used for producing messages) to the transaction coordinator.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"2.1.4"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator sends the result of adding a new produced partition to the transaction.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"2.2.1"),(0,o.kt)("td",{parentName:"tr",align:null},"The Pulsar client starts producing messages to partitions. The flow of this part is the same as the normal flow of producing messages except that the batch of messages produced by a transaction contains transaction IDs.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"2.2.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The broker writes messages to a partition.")))),(0,o.kt)("h3",{id:"3-acknowledge-messages-with-a-transaction"},"3. Acknowledge messages with a transaction"),(0,o.kt)("p",null,"In this phase, the Pulsar client sends a request to the transaction coordinator and a new subscription is acknowledged as a part of a transaction."),(0,o.kt)("p",null,(0,o.kt)("img",{src:a(15411).Z})),(0,o.kt)("p",null,"Let\u2019s walk through the steps for ",(0,o.kt)("em",{parentName:"p"},"acknowledging messages with a transaction"),"."),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Step"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"3.1.1"),(0,o.kt)("td",{parentName:"tr",align:null},"The Pulsar client sends a request to add an acknowledged subscription to the transaction coordinator.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"3.1.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator logs the addition of subscription, which ensures that it knows all subscriptions handled by a transaction and can commit or abort changes on each subscription at the end phase.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"3.1.3"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction log sends the result of logging the new partition (used for acknowledging messages) to the transaction coordinator.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"3.1.4"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator sends the result of adding the new acknowledged partition to the transaction.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"3.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The Pulsar client acknowledges messages on the subscription. The flow of this part is the same as the normal flow of acknowledging messages except that the acknowledged request carries a transaction ID.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"3.3"),(0,o.kt)("td",{parentName:"tr",align:null},"The broker receiving the acknowledgement request checks if the acknowledgment belongs to a transaction or not.")))),(0,o.kt)("h3",{id:"4-end-a-transaction"},"4. End a transaction"),(0,o.kt)("p",null,"At the end of a transaction, the Pulsar client decides to commit or abort the transaction. The transaction can be aborted when a conflict is detected on acknowledging messages. "),(0,o.kt)("h4",{id:"41-end-transaction-request"},"4.1 End transaction request"),(0,o.kt)("p",null,"When the Pulsar client finishes a transaction, it issues an end transaction request."),(0,o.kt)("p",null,(0,o.kt)("img",{src:a(79396).Z})),(0,o.kt)("p",null,"Let\u2019s walk through the steps for ",(0,o.kt)("em",{parentName:"p"},"ending the transaction"),"."),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Step"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.1.1"),(0,o.kt)("td",{parentName:"tr",align:null},"The Pulsar client issues an end transaction request (with a field indicating whether the transaction is to be committed or aborted) to the transaction coordinator.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.1.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator writes a COMMITTING or ABORTING message to its transaction log.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.1.3"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction log sends the result of logging the committing or aborting status.")))),(0,o.kt)("h4",{id:"42-finalize-a-transaction"},"4.2 Finalize a transaction"),(0,o.kt)("p",null,"The transaction coordinator starts the process of committing or aborting messages to all the partitions involved in this transaction. "),(0,o.kt)("p",null,(0,o.kt)("img",{src:a(85270).Z})),(0,o.kt)("p",null,"Let\u2019s walk through the steps for ",(0,o.kt)("em",{parentName:"p"},"finalizing a transaction"),"."),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Step"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.2.1"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator commits transactions on subscriptions and commits transactions on partitions at the same time.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.2.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The broker (produce) writes produced committed markers to the actual partitions. At the same time, the broker (ack) writes acked committed marks to the subscription pending ack partitions.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.2.3"),(0,o.kt)("td",{parentName:"tr",align:null},"The data log sends the result of writing produced committed marks to the broker. At the same time, pending ack data log sends the result of writing acked committed marks to the broker. The cursor moves to the next position.")))),(0,o.kt)("h4",{id:"43-mark-a-transaction-as-committed-or-aborted"},"4.3 Mark a transaction as COMMITTED or ABORTED"),(0,o.kt)("p",null,"The transaction coordinator writes the final transaction status to the transaction log to complete the transaction."),(0,o.kt)("p",null,(0,o.kt)("img",{src:a(37878).Z})),(0,o.kt)("p",null,"Let\u2019s walk through the steps for ",(0,o.kt)("em",{parentName:"p"},"marking a transaction as COMMITTED or ABORTED"),"."),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Step"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.3.1"),(0,o.kt)("td",{parentName:"tr",align:null},"After all produced messages and acknowledgements to all partitions involved in this transaction have been successfully committed or aborted, the transaction coordinator writes the final COMMITTED or ABORTED transaction status messages to its transaction log, indicating that the transaction is complete. All the messages associated with the transaction in its transaction log can be safely removed.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.3.2"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction log sends the result of the committed transaction to the transaction coordinator.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"4.3.3"),(0,o.kt)("td",{parentName:"tr",align:null},"The transaction coordinator sends the result of the committed transaction to the Pulsar client.")))))}h.isMDXComponent=!0},32805:function(t,e,a){e.Z=a.p+"assets/images/txn-3-751a2bc51f91299f6c546b647c2f632c.png"},5014:function(t,e,a){e.Z=a.p+"assets/images/txn-4-f7adc6fb4ff184199a981fc32dd2311e.png"},15411:function(t,e,a){e.Z=a.p+"assets/images/txn-5-66e33b5b6ba3d900a1635cb268a38b35.png"},79396:function(t,e,a){e.Z=a.p+"assets/images/txn-6-ac44126d5410be548e44717d2cc056fa.png"},85270:function(t,e,a){e.Z=a.p+"assets/images/txn-7-229fdd1904b8c411e77d48fe1c3fee65.png"},37878:function(t,e,a){e.Z=a.p+"assets/images/txn-8-d49405f853142c9762c4caaa8f862b4e.png"}}]);
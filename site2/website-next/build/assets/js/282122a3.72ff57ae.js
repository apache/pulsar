"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[58304],{3905:function(e,t,a){a.d(t,{Zo:function(){return c},kt:function(){return d}});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),p=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},c=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),m=p(a),d=r,h=m["".concat(s,".").concat(d)]||m[d]||u[d]||o;return a?n.createElement(h,i(i({ref:t},c),{},{components:a})):n.createElement(h,i({ref:t},c))}));function d(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,i=new Array(o);i[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:r,i[1]=l;for(var p=2;p<o;p++)i[p]=a[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},58215:function(e,t,a){var n=a(67294);t.Z=function(e){var t=e.children,a=e.hidden,r=e.className;return n.createElement("div",{role:"tabpanel",hidden:a,className:r},t)}},55064:function(e,t,a){a.d(t,{Z:function(){return p}});var n=a(67294),r=a(79443);var o=function(){var e=(0,n.useContext)(r.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},i=a(86010),l="tabItem_1uMI",s="tabItemActive_2DSg";var p=function(e){var t,a=e.lazy,r=e.block,p=e.defaultValue,c=e.values,u=e.groupId,m=e.className,d=n.Children.toArray(e.children),h=null!=c?c:d.map((function(e){return{value:e.props.value,label:e.props.label}})),k=null!=p?p:null==(t=d.find((function(e){return e.props.default})))?void 0:t.props.value,g=o(),f=g.tabGroupChoices,v=g.setTabGroupChoices,N=(0,n.useState)(k),b=N[0],y=N[1],w=[];if(null!=u){var C=f[u];null!=C&&C!==b&&h.some((function(e){return e.value===C}))&&y(C)}var x=function(e){var t=e.currentTarget,a=w.indexOf(t),n=h[a].value;y(n),null!=u&&(v(u,n),setTimeout((function(){var e,a,n,r,o,i,l,p;(e=t.getBoundingClientRect(),a=e.top,n=e.left,r=e.bottom,o=e.right,i=window,l=i.innerHeight,p=i.innerWidth,a>=0&&o<=p&&r<=l&&n>=0)||(t.scrollIntoView({block:"center",behavior:"smooth"}),t.classList.add(s),setTimeout((function(){return t.classList.remove(s)}),2e3))}),150))},j=function(e){var t,a=null;switch(e.key){case"ArrowRight":var n=w.indexOf(e.target)+1;a=w[n]||w[0];break;case"ArrowLeft":var r=w.indexOf(e.target)-1;a=w[r]||w[w.length-1]}null==(t=a)||t.focus()};return n.createElement("div",{className:"tabs-container"},n.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":r},m)},h.map((function(e){var t=e.value,a=e.label;return n.createElement("li",{role:"tab",tabIndex:b===t?0:-1,"aria-selected":b===t,className:(0,i.Z)("tabs__item",l,{"tabs__item--active":b===t}),key:t,ref:function(e){return w.push(e)},onKeyDown:j,onFocus:x,onClick:x},null!=a?a:t)}))),a?(0,n.cloneElement)(d.filter((function(e){return e.props.value===b}))[0],{className:"margin-vert--md"}):n.createElement("div",{className:"margin-vert--md"},d.map((function(e,t){return(0,n.cloneElement)(e,{key:t,hidden:e.props.value!==b})}))))}},79443:function(e,t,a){var n=(0,a(67294).createContext)(void 0);t.Z=n},80399:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return l},contentTitle:function(){return s},metadata:function(){return p},toc:function(){return c},default:function(){return m}});var n=a(87462),r=a(63366),o=(a(67294),a(3905)),i=(a(55064),a(58215),["components"]),l={id:"io-develop",title:"How to develop Pulsar connectors",sidebar_label:"Develop",original_id:"io-develop"},s=void 0,p={unversionedId:"io-develop",id:"version-2.7.0/io-develop",isDocsHomePage:!1,title:"How to develop Pulsar connectors",description:"This guide describes how to develop Pulsar connectors to move data",source:"@site/versioned_docs/version-2.7.0/io-develop.md",sourceDirName:".",slug:"/io-develop",permalink:"/docs/2.7.0/io-develop",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.0/io-develop.md",tags:[],version:"2.7.0",frontMatter:{id:"io-develop",title:"How to develop Pulsar connectors",sidebar_label:"Develop",original_id:"io-develop"},sidebar:"version-2.7.0/docsSidebar",previous:{title:"CDC connector",permalink:"/docs/2.7.0/io-cdc"},next:{title:"CLI",permalink:"/docs/2.7.0/io-cli"}},c=[{value:"Develop",id:"develop",children:[{value:"Source",id:"source",children:[]},{value:"Sink",id:"sink",children:[]}]},{value:"Test",id:"test",children:[{value:"Unit test",id:"unit-test",children:[]},{value:"Integration test",id:"integration-test",children:[]}]},{value:"Package",id:"package",children:[{value:"NAR",id:"nar",children:[]},{value:"Uber JAR",id:"uber-jar",children:[]}]}],u={toc:c};function m(e){var t=e.components,a=(0,r.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},u,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"This guide describes how to develop Pulsar connectors to move data\nbetween Pulsar and other systems. "),(0,o.kt)("p",null,"Pulsar connectors are special ",(0,o.kt)("a",{parentName:"p",href:"functions-overview"},"Pulsar Functions"),", so creating\na Pulsar connector is similar to creating a Pulsar function. "),(0,o.kt)("p",null,"Pulsar connectors come in two types: "),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Type"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"),(0,o.kt)("th",{parentName:"tr",align:null},"Example"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("a",{parentName:"td",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java"},"Source")),(0,o.kt)("td",{parentName:"tr",align:null},"Import data from another system to Pulsar."),(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("a",{parentName:"td",href:"io-rabbitmq"},"RabbitMQ source connector")," imports the messages of a RabbitMQ queue to a Pulsar topic.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("a",{parentName:"td",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java"},"Sink")),(0,o.kt)("td",{parentName:"tr",align:null},"Export data from Pulsar to another system."),(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("a",{parentName:"td",href:"io-kinesis"},"Kinesis sink connector")," exports the messages of a Pulsar topic to a Kinesis stream.")))),(0,o.kt)("h2",{id:"develop"},"Develop"),(0,o.kt)("p",null,"You can develop Pulsar source connectors and sink connectors."),(0,o.kt)("h3",{id:"source"},"Source"),(0,o.kt)("p",null,"Developing a source connector is to implement the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java"},"Source"),"\ninterface, which means you need to implement the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java"},"open")," method and the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java"},"read")," method."),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Implement the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java"},"open")," method. "),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-java"},"\n/**\n* Open connector with configuration\n*\n* @param config initialization config\n* @param sourceContext\n* @throws Exception IO type exceptions when opening a connector\n*/\nvoid open(final Map<String, Object> config, SourceContext sourceContext) throws Exception;\n\n")),(0,o.kt)("p",{parentName:"li"},"This method is called when the source connector is initialized. "),(0,o.kt)("p",{parentName:"li"},"In this method, you can retrieve all connector specific settings through the passed-in ",(0,o.kt)("inlineCode",{parentName:"p"},"config")," parameter and initialize all necessary resources. "),(0,o.kt)("p",{parentName:"li"},"For example, a Kafka connector can create a Kafka client in this ",(0,o.kt)("inlineCode",{parentName:"p"},"open")," method."),(0,o.kt)("p",{parentName:"li"},"Besides, Pulsar runtime also provides a ",(0,o.kt)("inlineCode",{parentName:"p"},"SourceContext")," for the\nconnector to access runtime resources for tasks like collecting metrics. The implementation can save the ",(0,o.kt)("inlineCode",{parentName:"p"},"SourceContext")," for future use.")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Implement the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java"},"read")," method."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-java"},"\n    /**\n    * Reads the next message from source.\n    * If source does not have any new messages, this call should block.\n    * @return next message from source.  The return result should never be null\n    * @throws Exception\n    */\n    Record<T> read() throws Exception;\n\n")),(0,o.kt)("p",{parentName:"li"},"If nothing to return, the implementation should be blocking rather than returning ",(0,o.kt)("inlineCode",{parentName:"p"},"null"),". "),(0,o.kt)("p",{parentName:"li"},"The returned ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java"},"Record")," should encapsulate the following information, which is needed by Pulsar IO runtime. "),(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java"},"Record")," should provide the following variables:"),(0,o.kt)("table",{parentName:"li"},(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Variable"),(0,o.kt)("th",{parentName:"tr",align:null},"Required"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"TopicName")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"Pulsar topic name from which the record is originated from.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"Key")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"Messages can optionally be tagged with keys.",(0,o.kt)("br",null),(0,o.kt)("br",null),"For more information, see ",(0,o.kt)("a",{parentName:"td",href:"/docs/2.7.0/concepts-messaging#routing-modes"},"Routing modes"),".")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"Value")),(0,o.kt)("td",{parentName:"tr",align:null},"Yes"),(0,o.kt)("td",{parentName:"tr",align:null},"Actual data of the record.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"EventTime")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"Event time of the record from the source.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"PartitionId")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"If the record is originated from a partitioned source, it returns its ",(0,o.kt)("inlineCode",{parentName:"td"},"PartitionId"),". ",(0,o.kt)("br",null),(0,o.kt)("br",null),(0,o.kt)("inlineCode",{parentName:"td"},"PartitionId")," is used as a part of the unique identifier by Pulsar IO runtime to deduplicate messages and achieve exactly-once processing guarantee.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"RecordSequence")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"If the record is originated from a sequential source, it returns its ",(0,o.kt)("inlineCode",{parentName:"td"},"RecordSequence"),".",(0,o.kt)("br",null),(0,o.kt)("br",null),(0,o.kt)("inlineCode",{parentName:"td"},"RecordSequence")," is used as a part of the unique identifier by Pulsar IO runtime to deduplicate messages and achieve exactly-once processing guarantee.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"Properties")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"If the record carries user-defined properties, it returns those properties.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"DestinationTopic")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"Topic to which message should be written.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"Message")),(0,o.kt)("td",{parentName:"tr",align:null},"No"),(0,o.kt)("td",{parentName:"tr",align:null},"A class which carries data sent by users.",(0,o.kt)("br",null),(0,o.kt)("br",null),"For more information, see ",(0,o.kt)("a",{parentName:"td",href:"https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/Message.java"},"Message.java"),"."))))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java"},"Record")," should provide the following methods:"),(0,o.kt)("table",{parentName:"li"},(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Method"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"ack")),(0,o.kt)("td",{parentName:"tr",align:null},"Acknowledge that the record is fully processed.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"fail")),(0,o.kt)("td",{parentName:"tr",align:null},"Indicate that the record fails to be processed.")))))))),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about ",(0,o.kt)("strong",{parentName:"p"},"how to create a source connector"),", see ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java"},"KafkaSource"),"."))),(0,o.kt)("h3",{id:"sink"},"Sink"),(0,o.kt)("p",null,"Developing a sink connector ",(0,o.kt)("strong",{parentName:"p"},"is similar to")," developing a source connector, that is, you need to implement the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java"},"Sink")," interface, which means implementing the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java"},"open")," method and the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java"},"write")," method."),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Implement the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java"},"open")," method."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-java"},"\n    /**\n    * Open connector with configuration\n    *\n    * @param config initialization config\n    * @param sinkContext\n    * @throws Exception IO type exceptions when opening a connector\n    */\n    void open(final Map<String, Object> config, SinkContext sinkContext) throws Exception;\n\n"))),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Implement the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java"},"write")," method."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-java"},"\n    /**\n    * Write a message to Sink\n    * @param record record to write to sink\n    * @throws Exception\n    */\n    void write(Record<T> record) throws Exception;\n\n")),(0,o.kt)("p",{parentName:"li"},"During the implementation, you can decide how to write the ",(0,o.kt)("inlineCode",{parentName:"p"},"Value")," and\nthe ",(0,o.kt)("inlineCode",{parentName:"p"},"Key")," to the actual source, and leverage all the provided information such as\n",(0,o.kt)("inlineCode",{parentName:"p"},"PartitionId")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"RecordSequence")," to achieve different processing guarantees. "),(0,o.kt)("p",{parentName:"li"},"You also need to ack records (if messages are sent successfully) or fail records (if messages fail to send). "))),(0,o.kt)("h2",{id:"test"},"Test"),(0,o.kt)("p",null,"Testing connectors can be challenging because Pulsar IO connectors interact with two systems\nthat may be difficult to mock\u2014Pulsar and the system to which the connector is connecting. "),(0,o.kt)("p",null,"It is\nrecommended writing special tests to test the connector functionalities as below\nwhile mocking the external service. "),(0,o.kt)("h3",{id:"unit-test"},"Unit test"),(0,o.kt)("p",null,"You can create unit tests for your connector."),(0,o.kt)("h3",{id:"integration-test"},"Integration test"),(0,o.kt)("p",null,"Once you have written sufficient unit tests, you can add\nseparate integration tests to verify end-to-end functionality. "),(0,o.kt)("p",null,"Pulsar uses ",(0,o.kt)("a",{parentName:"p",href:"https://www.testcontainers.org/"},"testcontainers")," ",(0,o.kt)("strong",{parentName:"p"},"for all integration tests"),". "),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about ",(0,o.kt)("strong",{parentName:"p"},"how to create integration tests for Pulsar connectors"),", see ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//tests/integration/src/test/java/org/apache/pulsar/tests/integration/io"},"IntegrationTests"),"."))),(0,o.kt)("h2",{id:"package"},"Package"),(0,o.kt)("p",null,"Once you've developed and tested your connector, you need to package it so that it can be submitted\nto a ",(0,o.kt)("a",{parentName:"p",href:"functions-overview"},"Pulsar Functions")," cluster. "),(0,o.kt)("p",null,"There are two methods to\nwork with Pulsar Functions' runtime, that is, ",(0,o.kt)("a",{parentName:"p",href:"#nar"},"NAR")," and ",(0,o.kt)("a",{parentName:"p",href:"#uber-jar"},"uber JAR"),"."),(0,o.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"If you plan to package and distribute your connector for others to use, you are obligated to"))),(0,o.kt)("p",null,"license and copyright your own code properly. Remember to add the license and copyright to\nall libraries your code uses and to your distribution. "),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"If you use the ",(0,o.kt)("a",{parentName:"p",href:"#nar"},"NAR")," method, the NAR plugin\nautomatically creates a ",(0,o.kt)("inlineCode",{parentName:"p"},"DEPENDENCIES")," file in the generated NAR package, including the proper\nlicensing and copyrights of all libraries of your connector.")),(0,o.kt)("h3",{id:"nar"},"NAR"),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"NAR")," stands for NiFi Archive, which is a custom packaging mechanism used by Apache NiFi, to provide\na bit of Java ClassLoader isolation. "),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about ",(0,o.kt)("strong",{parentName:"p"},"how NAR works"),", see ",(0,o.kt)("a",{parentName:"p",href:"https://medium.com/hashmapinc/nifi-nar-files-explained-14113f7796fd"},"here"),". "))),(0,o.kt)("p",null,"Pulsar uses the same mechanism for packaging ",(0,o.kt)("strong",{parentName:"p"},"all")," ",(0,o.kt)("a",{parentName:"p",href:"io-connectors"},"built-in connectors"),". "),(0,o.kt)("p",null,"The easiest approach to package a Pulsar connector is to create a NAR package using ",(0,o.kt)("a",{parentName:"p",href:"https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin"},"nifi-nar-maven-plugin"),"."),(0,o.kt)("p",null,"Include this ",(0,o.kt)("a",{parentName:"p",href:"https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin"},"nifi-nar-maven-plugin")," in your maven project for your connector as below. "),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-xml"},"\n<plugins>\n  <plugin>\n    <groupId>org.apache.nifi</groupId>\n    <artifactId>nifi-nar-maven-plugin</artifactId>\n    <version>1.2.0</version>\n  </plugin>\n</plugins>\n\n")),(0,o.kt)("p",null,"You must also create a ",(0,o.kt)("inlineCode",{parentName:"p"},"resources/META-INF/services/pulsar-io.yaml")," file with the following contents:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},"\nname: connector name\ndescription: connector description\nsourceClass: fully qualified class name (only if source connector)\nsinkClass: fully qualified class name (only if sink connector)\n\n")),(0,o.kt)("p",null,"For Gradle users, there is a ",(0,o.kt)("a",{parentName:"p",href:"https://plugins.gradle.org/plugin/io.github.lhotari.gradle-nar-plugin"},"Gradle Nar plugin available on the Gradle Plugin Portal"),"."),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about an ",(0,o.kt)("strong",{parentName:"p"},"how to use NAR for Pulsar connectors"),", see ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master//pulsar-io/twitter/pom.xml"},"TwitterFirehose"),"."))),(0,o.kt)("h3",{id:"uber-jar"},"Uber JAR"),(0,o.kt)("p",null,"An alternative approach is to create an ",(0,o.kt)("strong",{parentName:"p"},"uber JAR")," that contains all of the connector's JAR files\nand other resource files. No directory internal structure is necessary."),(0,o.kt)("p",null,"You can use ",(0,o.kt)("a",{parentName:"p",href:"https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html"},"maven-shade-plugin")," to create a uber JAR as below:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-xml"},"\n<plugin>\n  <groupId>org.apache.maven.plugins</groupId>\n  <artifactId>maven-shade-plugin</artifactId>\n  <version>3.1.1</version>\n  <executions>\n    <execution>\n      <phase>package</phase>\n      <goals>\n        <goal>shade</goal>\n      </goals>\n      <configuration>\n        <filters>\n          <filter>\n            <artifact>*:*</artifact>\n          </filter>\n        </filters>\n      </configuration>\n    </execution>\n  </executions>\n</plugin>\n\n")))}m.isMDXComponent=!0},86010:function(e,t,a){function n(e){var t,a,r="";if("string"==typeof e||"number"==typeof e)r+=e;else if("object"==typeof e)if(Array.isArray(e))for(t=0;t<e.length;t++)e[t]&&(a=n(e[t]))&&(r&&(r+=" "),r+=a);else for(t in e)e[t]&&(r&&(r+=" "),r+=t);return r}function r(){for(var e,t,a=0,r="";a<arguments.length;)(e=arguments[a++])&&(t=n(e))&&(r&&(r+=" "),r+=t);return r}a.d(t,{Z:function(){return r}})}}]);
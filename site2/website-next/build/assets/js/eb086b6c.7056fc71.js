"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[30789],{3905:function(e,n,t){t.d(n,{Zo:function(){return u},kt:function(){return m}});var a=t(67294);function r(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function o(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);n&&(a=a.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,a)}return t}function s(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?o(Object(t),!0).forEach((function(n){r(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function i(e,n){if(null==e)return{};var t,a,r=function(e,n){if(null==e)return{};var t,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)t=o[a],n.indexOf(t)>=0||(r[t]=e[t]);return r}(e,n);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)t=o[a],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var l=a.createContext({}),c=function(e){var n=a.useContext(l),t=n;return e&&(t="function"==typeof e?e(n):s(s({},n),e)),t},u=function(e){var n=c(e.components);return a.createElement(l.Provider,{value:n},e.children)},p={inlineCode:"code",wrapper:function(e){var n=e.children;return a.createElement(a.Fragment,{},n)}},d=a.forwardRef((function(e,n){var t=e.components,r=e.mdxType,o=e.originalType,l=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),d=c(t),m=r,g=d["".concat(l,".").concat(m)]||d[m]||p[m]||o;return t?a.createElement(g,s(s({ref:n},u),{},{components:t})):a.createElement(g,s({ref:n},u))}));function m(e,n){var t=arguments,r=n&&n.mdxType;if("string"==typeof e||r){var o=t.length,s=new Array(o);s[0]=d;var i={};for(var l in n)hasOwnProperty.call(n,l)&&(i[l]=n[l]);i.originalType=e,i.mdxType="string"==typeof e?e:r,s[1]=i;for(var c=2;c<o;c++)s[c]=t[c];return a.createElement.apply(null,s)}return a.createElement.apply(null,t)}d.displayName="MDXCreateElement"},2164:function(e,n,t){t.r(n),t.d(n,{frontMatter:function(){return i},contentTitle:function(){return l},metadata:function(){return c},toc:function(){return u},default:function(){return d}});var a=t(87462),r=t(63366),o=(t(67294),t(3905)),s=["components"],i={id:"standalone-docker",title:"Set up a standalone Pulsar in Docker",sidebar_label:"Run Pulsar in Docker",original_id:"standalone-docker"},l=void 0,c={unversionedId:"standalone-docker",id:"version-2.7.3/standalone-docker",isDocsHomePage:!1,title:"Set up a standalone Pulsar in Docker",description:"For local development and testing, you can run Pulsar in standalone",source:"@site/versioned_docs/version-2.7.3/getting-started-docker.md",sourceDirName:".",slug:"/standalone-docker",permalink:"/docs/2.7.3/standalone-docker",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.3/getting-started-docker.md",tags:[],version:"2.7.3",frontMatter:{id:"standalone-docker",title:"Set up a standalone Pulsar in Docker",sidebar_label:"Run Pulsar in Docker",original_id:"standalone-docker"},sidebar:"version-2.7.3/docsSidebar",previous:{title:"Run Pulsar locally",permalink:"/docs/2.7.3/"},next:{title:"Run Pulsar in Kubernetes",permalink:"/docs/2.7.3/kubernetes-helm"}},u=[{value:"Start Pulsar in Docker",id:"start-pulsar-in-docker",children:[]},{value:"Use Pulsar in Docker",id:"use-pulsar-in-docker",children:[{value:"Consume a message",id:"consume-a-message",children:[]},{value:"Produce a message",id:"produce-a-message",children:[]}]},{value:"Get the topic statistics",id:"get-the-topic-statistics",children:[]}],p={toc:u};function d(e){var n=e.components,t=(0,r.Z)(e,s);return(0,o.kt)("wrapper",(0,a.Z)({},p,t,{components:n,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"For local development and testing, you can run Pulsar in standalone\nmode on your own machine within a Docker container."),(0,o.kt)("p",null,"If you have not installed Docker, download the ",(0,o.kt)("a",{parentName:"p",href:"https://www.docker.com/community-edition"},"Community edition"),"\nand follow the instructions for your OS."),(0,o.kt)("h2",{id:"start-pulsar-in-docker"},"Start Pulsar in Docker"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"For MacOS, Linux, and Windows:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ docker run -it \\\n-p 6650:6650 \\\n-p 8080:8080 \\\n--mount source=pulsardata,target=/pulsar/data \\\n--mount source=pulsarconf,target=/pulsar/conf \\\napachepulsar/pulsar:2.7.3 \\\nbin/pulsar standalone\n\n")))),(0,o.kt)("p",null,"A few things to note about this command:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},'The data, metadata, and configuration are persisted on Docker volumes in order to not start "fresh" every\ntime the container is restarted. For details on the volumes you can use ',(0,o.kt)("inlineCode",{parentName:"li"},"docker volume inspect <sourcename>")),(0,o.kt)("li",{parentName:"ul"},"For Docker on Windows make sure to configure it to use Linux containers")),(0,o.kt)("p",null,"If you start Pulsar successfully, you will see ",(0,o.kt)("inlineCode",{parentName:"p"},"INFO"),"-level log messages like this:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"\n2017-08-09 22:34:04,030 - INFO  - [main:WebService@213] - Web Service started at http://127.0.0.1:8080\n2017-08-09 22:34:04,038 - INFO  - [main:PulsarService@335] - messaging service is ready, bootstrap service on port=8080, broker url=pulsar://127.0.0.1:6650, cluster=standalone, configs=org.apache.pulsar.broker.ServiceConfiguration@4db60246\n...\n\n")),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"When you start a local standalone cluster, a ",(0,o.kt)("inlineCode",{parentName:"p"},"public/default")))),(0,o.kt)("p",null,"namespace is created automatically. The namespace is used for development purposes. All Pulsar topics are managed within namespaces.\nFor more information, see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/concepts-messaging#topics"},"Topics"),"."),(0,o.kt)("h2",{id:"use-pulsar-in-docker"},"Use Pulsar in Docker"),(0,o.kt)("p",null,"Pulsar offers client libraries for ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/client-libraries-java"},"Java"),", ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/client-libraries-go"},"Go"),", ",(0,o.kt)("a",{parentName:"p",href:"client-libraries-python"},"Python"),"\nand ",(0,o.kt)("a",{parentName:"p",href:"client-libraries-cpp"},"C++"),". If you're running a local standalone cluster, you can\nuse one of these root URLs to interact with your cluster:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"pulsar://localhost:6650")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"http://localhost:8080"))),(0,o.kt)("p",null,"The following example will guide you get started with Pulsar quickly by using the ",(0,o.kt)("a",{parentName:"p",href:"client-libraries-python"},"Python"),"\nclient API."),(0,o.kt)("p",null,"Install the Pulsar Python client library directly from ",(0,o.kt)("a",{parentName:"p",href:"https://pypi.org/project/pulsar-client/"},"PyPI"),":"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pip install pulsar-client\n\n")),(0,o.kt)("h3",{id:"consume-a-message"},"Consume a message"),(0,o.kt)("p",null,"Create a consumer and subscribe to the topic:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"\nimport pulsar\n\nclient = pulsar.Client('pulsar://localhost:6650')\nconsumer = client.subscribe('my-topic',\n                            subscription_name='my-sub')\n\nwhile True:\n    msg = consumer.receive()\n    print(\"Received message: '%s'\" % msg.data())\n    consumer.acknowledge(msg)\n\nclient.close()\n\n")),(0,o.kt)("h3",{id:"produce-a-message"},"Produce a message"),(0,o.kt)("p",null,"Now start a producer to send some test messages:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"\nimport pulsar\n\nclient = pulsar.Client('pulsar://localhost:6650')\nproducer = client.create_producer('my-topic')\n\nfor i in range(10):\n    producer.send(('hello-pulsar-%d' % i).encode('utf-8'))\n\nclient.close()\n\n")),(0,o.kt)("h2",{id:"get-the-topic-statistics"},"Get the topic statistics"),(0,o.kt)("p",null,"In Pulsar, you can use REST, Java, or command-line tools to control every aspect of the system.\nFor details on APIs, refer to ",(0,o.kt)("a",{parentName:"p",href:"admin-api-overview"},"Admin API Overview"),"."),(0,o.kt)("p",null,"In the simplest example, you can use curl to probe the stats for a particular topic:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ curl http://localhost:8080/admin/v2/persistent/public/default/my-topic/stats | python -m json.tool\n\n")),(0,o.kt)("p",null,"The output is something like this:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "averageMsgSize": 0.0,\n  "msgRateIn": 0.0,\n  "msgRateOut": 0.0,\n  "msgThroughputIn": 0.0,\n  "msgThroughputOut": 0.0,\n  "publishers": [\n    {\n      "address": "/172.17.0.1:35048",\n      "averageMsgSize": 0.0,\n      "clientVersion": "1.19.0-incubating",\n      "connectedSince": "2017-08-09 20:59:34.621+0000",\n      "msgRateIn": 0.0,\n      "msgThroughputIn": 0.0,\n      "producerId": 0,\n      "producerName": "standalone-0-1"\n    }\n  ],\n  "replication": {},\n  "storageSize": 16,\n  "subscriptions": {\n    "my-sub": {\n      "blockedSubscriptionOnUnackedMsgs": false,\n      "consumers": [\n        {\n          "address": "/172.17.0.1:35064",\n          "availablePermits": 996,\n          "blockedConsumerOnUnackedMsgs": false,\n          "clientVersion": "1.19.0-incubating",\n          "connectedSince": "2017-08-09 21:05:39.222+0000",\n          "consumerName": "166111",\n          "msgRateOut": 0.0,\n          "msgRateRedeliver": 0.0,\n          "msgThroughputOut": 0.0,\n          "unackedMessages": 0\n        }\n      ],\n      "msgBacklog": 0,\n      "msgRateExpired": 0.0,\n      "msgRateOut": 0.0,\n      "msgRateRedeliver": 0.0,\n      "msgThroughputOut": 0.0,\n      "type": "Exclusive",\n      "unackedMessages": 0\n    }\n  }\n}\n\n')))}d.isMDXComponent=!0}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[4466],{3905:function(e,a,t){t.d(a,{Zo:function(){return m},kt:function(){return c}});var r=t(67294);function n(e,a,t){return a in e?Object.defineProperty(e,a,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[a]=t,e}function o(e,a){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);a&&(r=r.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),t.push.apply(t,r)}return t}function i(e){for(var a=1;a<arguments.length;a++){var t=null!=arguments[a]?arguments[a]:{};a%2?o(Object(t),!0).forEach((function(a){n(e,a,t[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(t,a))}))}return e}function l(e,a){if(null==e)return{};var t,r,n=function(e,a){if(null==e)return{};var t,r,n={},o=Object.keys(e);for(r=0;r<o.length;r++)t=o[r],a.indexOf(t)>=0||(n[t]=e[t]);return n}(e,a);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)t=o[r],a.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(n[t]=e[t])}return n}var s=r.createContext({}),d=function(e){var a=r.useContext(s),t=a;return e&&(t="function"==typeof e?e(a):i(i({},a),e)),t},m=function(e){var a=d(e.components);return r.createElement(s.Provider,{value:a},e.children)},p={inlineCode:"code",wrapper:function(e){var a=e.children;return r.createElement(r.Fragment,{},a)}},u=r.forwardRef((function(e,a){var t=e.components,n=e.mdxType,o=e.originalType,s=e.parentName,m=l(e,["components","mdxType","originalType","parentName"]),u=d(t),c=n,h=u["".concat(s,".").concat(c)]||u[c]||p[c]||o;return t?r.createElement(h,i(i({ref:a},m),{},{components:t})):r.createElement(h,i({ref:a},m))}));function c(e,a){var t=arguments,n=a&&a.mdxType;if("string"==typeof e||n){var o=t.length,i=new Array(o);i[0]=u;var l={};for(var s in a)hasOwnProperty.call(a,s)&&(l[s]=a[s]);l.originalType=e,l.mdxType="string"==typeof e?e:n,i[1]=l;for(var d=2;d<o;d++)i[d]=t[d];return r.createElement.apply(null,i)}return r.createElement.apply(null,t)}u.displayName="MDXCreateElement"},58215:function(e,a,t){var r=t(67294);a.Z=function(e){var a=e.children,t=e.hidden,n=e.className;return r.createElement("div",{role:"tabpanel",hidden:t,className:n},a)}},55064:function(e,a,t){t.d(a,{Z:function(){return d}});var r=t(67294),n=t(79443);var o=function(){var e=(0,r.useContext)(n.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},i=t(86010),l="tabItem_1uMI",s="tabItemActive_2DSg";var d=function(e){var a,t=e.lazy,n=e.block,d=e.defaultValue,m=e.values,p=e.groupId,u=e.className,c=r.Children.toArray(e.children),h=null!=m?m:c.map((function(e){return{value:e.props.value,label:e.props.label}})),g=null!=d?d:null==(a=c.find((function(e){return e.props.default})))?void 0:a.props.value,b=o(),k=b.tabGroupChoices,f=b.setTabGroupChoices,v=(0,r.useState)(g),N=v[0],y=v[1],w=[];if(null!=p){var T=k[p];null!=T&&T!==N&&h.some((function(e){return e.value===T}))&&y(T)}var M=function(e){var a=e.currentTarget,t=w.indexOf(a),r=h[t].value;y(r),null!=p&&(f(p,r),setTimeout((function(){var e,t,r,n,o,i,l,d;(e=a.getBoundingClientRect(),t=e.top,r=e.left,n=e.bottom,o=e.right,i=window,l=i.innerHeight,d=i.innerWidth,t>=0&&o<=d&&n<=l&&r>=0)||(a.scrollIntoView({block:"center",behavior:"smooth"}),a.classList.add(s),setTimeout((function(){return a.classList.remove(s)}),2e3))}),150))},C=function(e){var a,t=null;switch(e.key){case"ArrowRight":var r=w.indexOf(e.target)+1;t=w[r]||w[0];break;case"ArrowLeft":var n=w.indexOf(e.target)-1;t=w[n]||w[w.length-1]}null==(a=t)||a.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":n},u)},h.map((function(e){var a=e.value,t=e.label;return r.createElement("li",{role:"tab",tabIndex:N===a?0:-1,"aria-selected":N===a,className:(0,i.Z)("tabs__item",l,{"tabs__item--active":N===a}),key:a,ref:function(e){return w.push(e)},onKeyDown:C,onFocus:M,onClick:M},null!=t?t:a)}))),t?(0,r.cloneElement)(c.filter((function(e){return e.props.value===N}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},c.map((function(e,a){return(0,r.cloneElement)(e,{key:a,hidden:e.props.value!==N})}))))}},79443:function(e,a,t){var r=(0,t(67294).createContext)(void 0);a.Z=r},70452:function(e,a,t){t.r(a),t.d(a,{frontMatter:function(){return l},contentTitle:function(){return s},metadata:function(){return d},toc:function(){return m},default:function(){return u}});var r=t(87462),n=t(63366),o=(t(67294),t(3905)),i=(t(55064),t(58215),["components"]),l={id:"develop-load-manager",title:"Modular load manager",sidebar_label:"Modular load manager",original_id:"develop-load-manager"},s=void 0,d={unversionedId:"develop-load-manager",id:"version-2.2.0/develop-load-manager",isDocsHomePage:!1,title:"Modular load manager",description:"The modular load manager, implemented in  ModularLoadManagerImpl, is a flexible alternative to the previously implemented load manager, SimpleLoadManagerImpl, which attempts to simplify how load is managed while also providing abstractions so that complex load management strategies may be implemented.",source:"@site/versioned_docs/version-2.2.0/developing-load-manager.md",sourceDirName:".",slug:"/develop-load-manager",permalink:"/docs/2.2.0/develop-load-manager",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.2.0/developing-load-manager.md",tags:[],version:"2.2.0",frontMatter:{id:"develop-load-manager",title:"Modular load manager",sidebar_label:"Modular load manager",original_id:"develop-load-manager"},sidebar:"version-2.2.0/docsSidebar",previous:{title:"Custom schema storage",permalink:"/docs/2.2.0/develop-schema"},next:{title:"Building Pulsar C++ client",permalink:"/docs/2.2.0/develop-cpp"}},m=[{value:"Usage",id:"usage",children:[]},{value:"Verification",id:"verification",children:[]},{value:"Implementation",id:"implementation",children:[{value:"Data",id:"data",children:[]},{value:"Traffic Distribution",id:"traffic-distribution",children:[]}]}],p={toc:m};function u(e){var a=e.components,t=(0,n.Z)(e,i);return(0,o.kt)("wrapper",(0,r.Z)({},p,t,{components:a,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"The ",(0,o.kt)("em",{parentName:"p"},"modular load manager"),", implemented in  ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/impl/ModularLoadManagerImpl.java"},(0,o.kt)("inlineCode",{parentName:"a"},"ModularLoadManagerImpl")),", is a flexible alternative to the previously implemented load manager, ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/impl/SimpleLoadManagerImpl.java"},(0,o.kt)("inlineCode",{parentName:"a"},"SimpleLoadManagerImpl")),", which attempts to simplify how load is managed while also providing abstractions so that complex load management strategies may be implemented."),(0,o.kt)("h2",{id:"usage"},"Usage"),(0,o.kt)("p",null,"There are two ways that you can enable the modular load manager:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Change the value of the ",(0,o.kt)("inlineCode",{parentName:"p"},"loadManagerClassName")," parameter in ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/broker.conf")," from ",(0,o.kt)("inlineCode",{parentName:"p"},"org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl")," to ",(0,o.kt)("inlineCode",{parentName:"p"},"org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl"),".")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Using the ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin")," tool. Here's an example:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin brokers update-dynamic-config \\\n  --config loadManagerClassName \\\n  --value org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl\n\n")),(0,o.kt)("p",{parentName:"li"},"You can use the same method to change back to the original value. In either case, any mistake in specifying the load manager will cause Pulsar to default to ",(0,o.kt)("inlineCode",{parentName:"p"},"SimpleLoadManagerImpl"),"."))),(0,o.kt)("h2",{id:"verification"},"Verification"),(0,o.kt)("p",null,"There are a few different ways to determine which load manager is being used:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Use ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin")," to examine the ",(0,o.kt)("inlineCode",{parentName:"p"},"loadManagerClassName")," element:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},'\n$ bin/pulsar-admin brokers get-all-dynamic-config\n{\n "loadManagerClassName" : "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl"\n}\n\n')),(0,o.kt)("p",{parentName:"li"},"If there is no ",(0,o.kt)("inlineCode",{parentName:"p"},"loadManagerClassName")," element, then the default load manager is used.")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Consult a ZooKeeper load report. With the module load manager, the load report in ",(0,o.kt)("inlineCode",{parentName:"p"},"/loadbalance/brokers/...")," will have many differences. for example the ",(0,o.kt)("inlineCode",{parentName:"p"},"systemResourceUsage")," sub-elements (",(0,o.kt)("inlineCode",{parentName:"p"},"bandwidthIn"),", ",(0,o.kt)("inlineCode",{parentName:"p"},"bandwidthOut"),", etc.) are now all at the top level. Here is an example load report from the module load manager:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "bandwidthIn": {\n    "limit": 10240000.0,\n    "usage": 4.256510416666667\n  },\n  "bandwidthOut": {\n    "limit": 10240000.0,\n    "usage": 5.287239583333333\n  },\n  "bundles": [],\n  "cpu": {\n    "limit": 2400.0,\n    "usage": 5.7353247655435915\n  },\n  "directMemory": {\n    "limit": 16384.0,\n    "usage": 1.0\n  }\n}\n\n')),(0,o.kt)("p",{parentName:"li"},"With the simple load manager, the load report in ",(0,o.kt)("inlineCode",{parentName:"p"},"/loadbalance/brokers/...")," will look like this:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "systemResourceUsage": {\n    "bandwidthIn": {\n      "limit": 10240000.0,\n      "usage": 0.0\n    },\n    "bandwidthOut": {\n      "limit": 10240000.0,\n      "usage": 0.0\n    },\n    "cpu": {\n      "limit": 2400.0,\n      "usage": 0.0\n    },\n    "directMemory": {\n      "limit": 16384.0,\n      "usage": 1.0\n    },\n    "memory": {\n      "limit": 8192.0,\n      "usage": 3903.0\n    }\n  }\n}\n\n'))),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"The command-line ",(0,o.kt)("a",{parentName:"p",href:"reference-cli-tools.md#monitor-brokers"},"broker monitor")," will have a different output format depending on which load manager implementation is being used."),(0,o.kt)("p",{parentName:"li"},"Here is an example from the modular load manager:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"===================================================================================================================\n||SYSTEM         |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||\n||               |0.00           |48.33          |0.01           |0.00           |0.00           |48.33          ||\n||COUNT          |TOPIC          |BUNDLE         |PRODUCER       |CONSUMER       |BUNDLE +       |BUNDLE -       ||\n||               |4              |4              |0              |2              |4              |0              ||\n||LATEST         |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||\n||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||\n||SHORT          |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||\n||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||\n||LONG           |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||\n||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||\n===================================================================================================================\n\n")),(0,o.kt)("p",{parentName:"li"},"Here is an example from the simple load manager:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"===================================================================================================================\n||COUNT          |TOPIC          |BUNDLE         |PRODUCER       |CONSUMER       |BUNDLE +       |BUNDLE -       ||\n||               |4              |4              |0              |2              |0              |0              ||\n||RAW SYSTEM     |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||\n||               |0.25           |47.94          |0.01           |0.00           |0.00           |47.94          ||\n||ALLOC SYSTEM   |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||\n||               |0.20           |1.89           |               |1.27           |3.21           |3.21           ||\n||RAW MSG        |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||\n||               |0.00           |0.00           |0.00           |0.01           |0.01           |0.01           ||\n||ALLOC MSG      |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||\n||               |54.84          |134.48         |189.31         |126.54         |320.96         |447.50         ||\n===================================================================================================================\n\n")))),(0,o.kt)("p",null,"It is important to note that the module load manager is ",(0,o.kt)("em",{parentName:"p"},"centralized"),", meaning that all requests to assign a bundle---whether it's been seen before or whether this is the first time---only get handled by the ",(0,o.kt)("em",{parentName:"p"},"lead")," broker (which can change over time). To determine the current lead broker, examine the ",(0,o.kt)("inlineCode",{parentName:"p"},"/loadbalance/leader")," node in ZooKeeper."),(0,o.kt)("h2",{id:"implementation"},"Implementation"),(0,o.kt)("h3",{id:"data"},"Data"),(0,o.kt)("p",null,"The data monitored by the modular load manager is contained in the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/LoadData.java"},(0,o.kt)("inlineCode",{parentName:"a"},"LoadData"))," class.\nHere, the available data is subdivided into the bundle data and the broker data."),(0,o.kt)("h4",{id:"broker"},"Broker"),(0,o.kt)("p",null,"The broker data is contained in the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/BrokerData.java"},(0,o.kt)("inlineCode",{parentName:"a"},"BrokerData"))," class. It is further subdivided into two parts,\none being the local data which every broker individually writes to ZooKeeper, and the other being the historical broker\ndata which is written to ZooKeeper by the leader broker."),(0,o.kt)("h5",{id:"local-broker-data"},"Local Broker Data"),(0,o.kt)("p",null,"The local broker data is contained in the class ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/java/org/apache/pulsar/policies/data/loadbalancer/LocalBrokerData.java"},(0,o.kt)("inlineCode",{parentName:"a"},"LocalBrokerData"))," and provides information about the following resources:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"CPU usage"),(0,o.kt)("li",{parentName:"ul"},"JVM heap memory usage"),(0,o.kt)("li",{parentName:"ul"},"Direct memory usage"),(0,o.kt)("li",{parentName:"ul"},"Bandwidth in/out usage"),(0,o.kt)("li",{parentName:"ul"},"Most recent total message rate in/out across all bundles"),(0,o.kt)("li",{parentName:"ul"},"Total number of topics, bundles, producers, and consumers"),(0,o.kt)("li",{parentName:"ul"},"Names of all bundles assigned to this broker"),(0,o.kt)("li",{parentName:"ul"},"Most recent changes in bundle assignments for this broker")),(0,o.kt)("p",null,'The local broker data is updated periodically according to the service configuration\n"loadBalancerReportUpdateMaxIntervalMinutes". After any broker updates their local broker data, the leader broker will\nreceive the update immediately via a ZooKeeper watch, where the local data is read from the ZooKeeper node\n',(0,o.kt)("inlineCode",{parentName:"p"},"/loadbalance/brokers/<broker host/port>")),(0,o.kt)("h5",{id:"historical-broker-data"},"Historical Broker Data"),(0,o.kt)("p",null,"The historical broker data is contained in the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/TimeAverageBrokerData.java"},(0,o.kt)("inlineCode",{parentName:"a"},"TimeAverageBrokerData"))," class."),(0,o.kt)("p",null,"In order to reconcile the need to make good decisions in a steady-state scenario and make reactive decisions in a critical scenario, the historical data is split into two parts: the short-term data for reactive decisions, and the long-term data for steady-state decisions. Both time frames maintain the following information:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Message rate in/out for the entire broker"),(0,o.kt)("li",{parentName:"ul"},"Message throughput in/out for the entire broker")),(0,o.kt)("p",null,"Unlike the bundle data, the broker data does not maintain samples for the global broker message rates and throughputs, which is not expected to remain steady as new bundles are removed or added. Instead, this data is aggregated over the short-term and long-term data for the bundles. See the section on bundle data to understand how that data is collected and maintained."),(0,o.kt)("p",null,"The historical broker data is updated for each broker in memory by the leader broker whenever any broker writes their local data to ZooKeeper. Then, the historical data is written to ZooKeeper by the leader broker periodically according to the configuration ",(0,o.kt)("inlineCode",{parentName:"p"},"loadBalancerResourceQuotaUpdateIntervalMinutes"),"."),(0,o.kt)("h5",{id:"bundle-data"},"Bundle Data"),(0,o.kt)("p",null,"The bundle data is contained in the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/BundleData.java"},(0,o.kt)("inlineCode",{parentName:"a"},"BundleData")),". Like the historical broker data, the bundle data is split into a short-term and a long-term time frame. The information maintained in each time frame:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Message rate in/out for this bundle"),(0,o.kt)("li",{parentName:"ul"},"Message Throughput In/Out for this bundle"),(0,o.kt)("li",{parentName:"ul"},"Current number of samples for this bundle")),(0,o.kt)("p",null,"The time frames are implemented by maintaining the average of these values over a set, limited number of samples, where\nthe samples are obtained through the message rate and throughput values in the local data. Thus, if the update interval\nfor the local data is 2 minutes, the number of short samples is 10 and the number of long samples is 1000, the\nshort-term data is maintained over a period of ",(0,o.kt)("inlineCode",{parentName:"p"},"10 samples * 2 minutes / sample = 20 minutes"),", while the long-term\ndata is similarly over a period of 2000 minutes. Whenever there are not enough samples to satisfy a given time frame,\nthe average is taken only over the existing samples. When no samples are available, default values are assumed until\nthey are overwritten by the first sample. Currently, the default values are"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Message rate in/out: 50 messages per second both ways"),(0,o.kt)("li",{parentName:"ul"},"Message throughput in/out: 50KB per second both ways")),(0,o.kt)("p",null,"The bundle data is updated in memory on the leader broker whenever any broker writes their local data to ZooKeeper.\nThen, the bundle data is written to ZooKeeper by the leader broker periodically at the same time as the historical\nbroker data, according to the configuration ",(0,o.kt)("inlineCode",{parentName:"p"},"loadBalancerResourceQuotaUpdateIntervalMinutes"),"."),(0,o.kt)("h3",{id:"traffic-distribution"},"Traffic Distribution"),(0,o.kt)("p",null,"The modular load manager uses the abstraction provided by ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/ModularLoadManagerStrategy.java"},(0,o.kt)("inlineCode",{parentName:"a"},"ModularLoadManagerStrategy"))," to make decisions about bundle assignment. The strategy makes a decision by considering the service configuration, the entire load data, and the bundle data for the bundle to be assigned. Currently, the only supported strategy is ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/impl/LeastLongTermMessageRate.java"},(0,o.kt)("inlineCode",{parentName:"a"},"LeastLongTermMessageRate")),", though soon users will have the ability to inject their own strategies if desired."),(0,o.kt)("h4",{id:"least-long-term-message-rate-strategy"},"Least Long Term Message Rate Strategy"),(0,o.kt)("p",null,"As its name suggests, the least long term message rate strategy attempts to distribute bundles across brokers so that\nthe message rate in the long-term time window for each broker is roughly the same. However, simply balancing load based\non message rate does not handle the issue of asymmetric resource burden per message on each broker. Thus, the system\nresource usages, which are CPU, memory, direct memory, bandwidth in, and bandwidth out, are also considered in the\nassignment process. This is done by weighting the final message rate according to\n",(0,o.kt)("inlineCode",{parentName:"p"},"1 / (overload_threshold - max_usage)"),", where ",(0,o.kt)("inlineCode",{parentName:"p"},"overload_threshold")," corresponds to the configuration\n",(0,o.kt)("inlineCode",{parentName:"p"},"loadBalancerBrokerOverloadedThresholdPercentage")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"max_usage")," is the maximum proportion among the system resources\nthat is being utilized by the candidate broker. This multiplier ensures that machines with are being more heavily taxed\nby the same message rates will receive less load. In particular, it tries to ensure that if one machine is overloaded,\nthen all machines are approximately overloaded. In the case in which a broker's max usage exceeds the overload\nthreshold, that broker is not considered for bundle assignment. If all brokers are overloaded, the bundle is randomly\nassigned."))}u.isMDXComponent=!0},86010:function(e,a,t){function r(e){var a,t,n="";if("string"==typeof e||"number"==typeof e)n+=e;else if("object"==typeof e)if(Array.isArray(e))for(a=0;a<e.length;a++)e[a]&&(t=r(e[a]))&&(n&&(n+=" "),n+=t);else for(a in e)e[a]&&(n&&(n+=" "),n+=a);return n}function n(){for(var e,a,t=0,n="";t<arguments.length;)(e=arguments[t++])&&(a=r(e))&&(n&&(n+=" "),n+=a);return n}t.d(a,{Z:function(){return n}})}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[98299],{3905:function(e,t,r){r.d(t,{Zo:function(){return p},kt:function(){return m}});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function s(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var l=n.createContext({}),c=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):s(s({},t),e)),r},p=function(e){var t=c(e.components);return n.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,l=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),d=c(r),m=a,h=d["".concat(l,".").concat(m)]||d[m]||u[m]||o;return r?n.createElement(h,s(s({ref:t},p),{},{components:r})):n.createElement(h,s({ref:t},p))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,s=new Array(o);s[0]=d;var i={};for(var l in t)hasOwnProperty.call(t,l)&&(i[l]=t[l]);i.originalType=e,i.mdxType="string"==typeof e?e:a,s[1]=i;for(var c=2;c<o;c++)s[c]=r[c];return n.createElement.apply(null,s)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},29496:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return i},contentTitle:function(){return l},metadata:function(){return c},toc:function(){return p},default:function(){return d}});var n=r(87462),a=r(63366),o=(r(67294),r(3905)),s=["components"],i={id:"deploy-monitoring",title:"Monitor",sidebar_label:"Monitor"},l=void 0,c={unversionedId:"deploy-monitoring",id:"deploy-monitoring",isDocsHomePage:!1,title:"Monitor",description:"You can use different ways to monitor a Pulsar cluster, exposing both metrics related to the usage of topics and the overall health of the individual components of the cluster.",source:"@site/docs/deploy-monitoring.md",sourceDirName:".",slug:"/deploy-monitoring",permalink:"/docs/next/deploy-monitoring",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/docs/deploy-monitoring.md",tags:[],version:"current",frontMatter:{id:"deploy-monitoring",title:"Monitor",sidebar_label:"Monitor"},sidebar:"docsSidebar",previous:{title:"Docker",permalink:"/docs/next/deploy-docker"},next:{title:"ZooKeeper and BookKeeper",permalink:"/docs/next/administration-zk-bk"}},p=[{value:"Collect metrics",id:"collect-metrics",children:[{value:"Broker stats",id:"broker-stats",children:[]},{value:"ZooKeeper stats",id:"zookeeper-stats",children:[]},{value:"BookKeeper stats",id:"bookkeeper-stats",children:[]},{value:"Managed cursor acknowledgment state",id:"managed-cursor-acknowledgment-state",children:[]},{value:"Function and connector stats",id:"function-and-connector-stats",children:[]}]},{value:"Configure Prometheus",id:"configure-prometheus",children:[]},{value:"Dashboards",id:"dashboards",children:[{value:"Pulsar per-topic dashboard",id:"pulsar-per-topic-dashboard",children:[]},{value:"Grafana",id:"grafana",children:[]}]},{value:"Alerting rules",id:"alerting-rules",children:[]}],u={toc:p};function d(e){var t=e.components,r=(0,a.Z)(e,s);return(0,o.kt)("wrapper",(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"You can use different ways to monitor a Pulsar cluster, exposing both metrics related to the usage of topics and the overall health of the individual components of the cluster."),(0,o.kt)("h2",{id:"collect-metrics"},"Collect metrics"),(0,o.kt)("p",null,"You can collect broker stats, ZooKeeper stats, and BookKeeper stats. "),(0,o.kt)("h3",{id:"broker-stats"},"Broker stats"),(0,o.kt)("p",null,"You can collect Pulsar broker metrics from brokers and export the metrics in JSON format. The Pulsar broker metrics mainly have two types:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("em",{parentName:"p"},"Destination dumps"),", which contain stats for each individual topic. You can fetch the destination dumps using the command below:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\nbin/pulsar-admin broker-stats destinations\n\n"))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Broker metrics, which contain the broker information and topics stats aggregated at namespace level. You can fetch the broker metrics by using the following command:"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\nbin/pulsar-admin broker-stats monitoring-metrics\n\n")))),(0,o.kt)("p",null,"All the message rates are updated every minute."),(0,o.kt)("p",null,"The aggregated broker metrics are also exposed in the ",(0,o.kt)("a",{parentName:"p",href:"https://prometheus.io"},"Prometheus")," format at:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\nhttp://$BROKER_ADDRESS:8080/metrics\n\n")),(0,o.kt)("h3",{id:"zookeeper-stats"},"ZooKeeper stats"),(0,o.kt)("p",null,"The local ZooKeeper, configuration store server and clients that are shipped with Pulsar can expose detailed stats through Prometheus."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\nhttp://$LOCAL_ZK_SERVER:8000/metrics\nhttp://$GLOBAL_ZK_SERVER:8001/metrics\n\n")),(0,o.kt)("p",null,"The default port of local ZooKeeper is ",(0,o.kt)("inlineCode",{parentName:"p"},"8000")," and the default port of configuration store is ",(0,o.kt)("inlineCode",{parentName:"p"},"8001"),". You can change the default port of local ZooKeeper and configuration store by specifying system property ",(0,o.kt)("inlineCode",{parentName:"p"},"stats_server_port"),"."),(0,o.kt)("h3",{id:"bookkeeper-stats"},"BookKeeper stats"),(0,o.kt)("p",null,"You can configure the stats frameworks for BookKeeper by modifying the ",(0,o.kt)("inlineCode",{parentName:"p"},"statsProviderClass")," in the ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/bookkeeper.conf")," file."),(0,o.kt)("p",null,"The default BookKeeper configuration enables the Prometheus exporter. The configuration is included with Pulsar distribution."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\nhttp://$BOOKIE_ADDRESS:8000/metrics\n\n")),(0,o.kt)("p",null,"The default port for bookie is ",(0,o.kt)("inlineCode",{parentName:"p"},"8000"),". You can change the port by configuring ",(0,o.kt)("inlineCode",{parentName:"p"},"prometheusStatsHttpPort")," in the ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/bookkeeper.conf")," file."),(0,o.kt)("h3",{id:"managed-cursor-acknowledgment-state"},"Managed cursor acknowledgment state"),(0,o.kt)("p",null,"The acknowledgment state is persistent to the ledger first. When the acknowledgment state fails to be persistent to the ledger, they are persistent to ZooKeeper. To track the stats of acknowledgement, you can configure the metrics for the managed cursor. "),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'\nbrk_ml_cursor_persistLedgerSucceed(namespace=", ledger_name="", cursor_name:")\nbrk_ml_cursor_persistLedgerErrors(namespace="", ledger_name="", cursor_name:"")\nbrk_ml_cursor_persistZookeeperSucceed(namespace="", ledger_name="", cursor_name:"")\nbrk_ml_cursor_persistZookeeperErrors(namespace="", ledger_name="", cursor_name:"")\nbrk_ml_cursor_nonContiguousDeletedMessagesRange(namespace="", ledger_name="", cursor_name:"")\n\n')),(0,o.kt)("p",null,"Those metrics are added in the Prometheus interface, you can monitor and check the metrics stats in the Grafana."),(0,o.kt)("h3",{id:"function-and-connector-stats"},"Function and connector stats"),(0,o.kt)("p",null,"You can collect functions worker stats from ",(0,o.kt)("inlineCode",{parentName:"p"},"functions-worker")," and export the metrics in JSON formats, which contain functions worker JVM metrics."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"\npulsar-admin functions-worker monitoring-metrics\n\n")),(0,o.kt)("p",null,"You can collect functions and connectors metrics from ",(0,o.kt)("inlineCode",{parentName:"p"},"functions-worker")," and export the metrics in JSON formats."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"\npulsar-admin functions-worker function-stats\n\n")),(0,o.kt)("p",null,"The aggregated functions and connectors metrics can be exposed in Prometheus formats as below. You can get ",(0,o.kt)("a",{parentName:"p",href:"http://pulsar.apache.org/docs/en/next/functions-worker/"},(0,o.kt)("inlineCode",{parentName:"a"},"FUNCTIONS_WORKER_ADDRESS"))," and ",(0,o.kt)("inlineCode",{parentName:"p"},"WORKER_PORT")," from the ",(0,o.kt)("inlineCode",{parentName:"p"},"functions_worker.yml")," file."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"\nhttp://$FUNCTIONS_WORKER_ADDRESS:$WORKER_PORT/metrics:\n\n")),(0,o.kt)("h2",{id:"configure-prometheus"},"Configure Prometheus"),(0,o.kt)("p",null,"You can use Prometheus to collect all the metrics exposed for Pulsar components and set up ",(0,o.kt)("a",{parentName:"p",href:"https://grafana.com/"},"Grafana")," dashboards to display the metrics and monitor your Pulsar cluster. For details, refer to ",(0,o.kt)("a",{parentName:"p",href:"https://prometheus.io/docs/introduction/getting_started/"},"Prometheus guide"),"."),(0,o.kt)("p",null,"When you run Pulsar on bare metal, you can provide the list of nodes to be probed. When you deploy Pulsar in a Kubernetes cluster, the monitoring is setup automatically. For details, refer to ",(0,o.kt)("a",{parentName:"p",href:"helm-deploy"},"Kubernetes instructions"),". "),(0,o.kt)("h2",{id:"dashboards"},"Dashboards"),(0,o.kt)("p",null,"When you collect time series statistics, the major problem is to make sure the number of dimensions attached to the data does not explode. Thus you only need to collect time series of metrics aggregated at the namespace level."),(0,o.kt)("h3",{id:"pulsar-per-topic-dashboard"},"Pulsar per-topic dashboard"),(0,o.kt)("p",null,"The per-topic dashboard instructions are available at ",(0,o.kt)("a",{parentName:"p",href:"administration-pulsar-manager"},"Pulsar manager"),"."),(0,o.kt)("h3",{id:"grafana"},"Grafana"),(0,o.kt)("p",null,"You can use grafana to create dashboard driven by the data that is stored in Prometheus."),(0,o.kt)("p",null,"When you deploy Pulsar on Kubernetes, a ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-grafana")," Docker image is enabled by default. You can use the docker image with the principal dashboards."),(0,o.kt)("p",null,"Enter the command below to use the dashboard manually:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\ndocker run -p3000:3000 \\\n        -e PROMETHEUS_URL=http://$PROMETHEUS_HOST:9090/ \\\n        apachepulsar/pulsar-grafana:latest\n\n")),(0,o.kt)("p",null,"The following are some Grafana dashboards examples:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"http://pulsar.apache.org/docs/en/deploy-monitoring/#grafana"},"pulsar-grafana"),": a Grafana dashboard that displays metrics collected in Prometheus for Pulsar clusters running on Kubernetes."),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://github.com/streamnative/apache-pulsar-grafana-dashboard"},"apache-pulsar-grafana-dashboard"),": a collection of Grafana dashboard templates for different Pulsar components running on both Kubernetes and on-premise machines.")),(0,o.kt)("h2",{id:"alerting-rules"},"Alerting rules"),(0,o.kt)("p",null,"You can set alerting rules according to your Pulsar environment. To configure alerting rules for Apache Pulsar, refer to ",(0,o.kt)("a",{parentName:"p",href:"https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/"},"alerting rules"),"."))}d.isMDXComponent=!0}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[87995],{3905:function(e,t,r){r.d(t,{Zo:function(){return p},kt:function(){return f}});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var l=n.createContext({}),c=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},p=function(e){var t=c(e.components);return n.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),m=c(r),f=a,d=m["".concat(l,".").concat(f)]||m[f]||u[f]||i;return r?n.createElement(d,o(o({ref:t},p),{},{components:r})):n.createElement(d,o({ref:t},p))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:a,o[1]=s;for(var c=2;c<i;c++)o[c]=r[c];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},83390:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return c},toc:function(){return p},default:function(){return m}});var n=r(87462),a=r(63366),i=(r(67294),r(3905)),o=["components"],s={id:"concepts-overview",title:"Pulsar Overview",sidebar_label:"Overview",original_id:"concepts-overview"},l=void 0,c={unversionedId:"concepts-overview",id:"version-2.6.3/concepts-overview",isDocsHomePage:!1,title:"Pulsar Overview",description:"Pulsar is a multi-tenant, high-performance solution for server-to-server messaging. Pulsar was originally developed by Yahoo, it is under the stewardship of the Apache Software Foundation.",source:"@site/versioned_docs/version-2.6.3/concepts-overview.md",sourceDirName:".",slug:"/concepts-overview",permalink:"/docs/2.6.3/concepts-overview",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.3/concepts-overview.md",tags:[],version:"2.6.3",frontMatter:{id:"concepts-overview",title:"Pulsar Overview",sidebar_label:"Overview",original_id:"concepts-overview"},sidebar:"version-2.6.3/docsSidebar",previous:{title:"Use Pulsar with client libraries",permalink:"/docs/2.6.3/client-libraries"},next:{title:"Messaging",permalink:"/docs/2.6.3/concepts-messaging"}},p=[{value:"Contents",id:"contents",children:[]}],u={toc:p};function m(e){var t=e.components,r=(0,a.Z)(e,o);return(0,i.kt)("wrapper",(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Pulsar is a multi-tenant, high-performance solution for server-to-server messaging. Pulsar was originally developed by Yahoo, it is under the stewardship of the ",(0,i.kt)("a",{parentName:"p",href:"https://www.apache.org/"},"Apache Software Foundation"),"."),(0,i.kt)("p",null,"Key features of Pulsar are listed below:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Native support for multiple clusters in a Pulsar instance, with seamless ",(0,i.kt)("a",{parentName:"li",href:"administration-geo"},"geo-replication")," of messages across clusters."),(0,i.kt)("li",{parentName:"ul"},"Very low publish and end-to-end latency."),(0,i.kt)("li",{parentName:"ul"},"Seamless scalability to over a million topics."),(0,i.kt)("li",{parentName:"ul"},"A simple ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/concepts-clients"},"client API")," with bindings for ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/client-libraries-java"},"Java"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/client-libraries-go"},"Go"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/client-libraries-python"},"Python")," and ",(0,i.kt)("a",{parentName:"li",href:"client-libraries-cpp"},"C++"),"."),(0,i.kt)("li",{parentName:"ul"},"Multiple ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/concepts-messaging#subscription-types"},"subscription types")," (",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/concepts-messaging#exclusive"},"exclusive"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/concepts-messaging#shared"},"shared"),", and ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/concepts-messaging#failover"},"failover"),") for topics."),(0,i.kt)("li",{parentName:"ul"},"Guaranteed message delivery with ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/concepts-architecture-overview#persistent-storage"},"persistent message storage")," provided by ",(0,i.kt)("a",{parentName:"li",href:"http://bookkeeper.apache.org/"},"Apache BookKeeper"),"."),(0,i.kt)("li",{parentName:"ul"},"A serverless light-weight computing framework ",(0,i.kt)("a",{parentName:"li",href:"functions-overview"},"Pulsar Functions")," offers the capability for stream-native data processing."),(0,i.kt)("li",{parentName:"ul"},"A serverless connector framework ",(0,i.kt)("a",{parentName:"li",href:"io-overview"},"Pulsar IO"),", which is built on Pulsar Functions, makes it easier to move data in and out of Apache Pulsar."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-tiered-storage"},"Tiered Storage")," offloads data from hot/warm storage to cold/longterm storage (such as S3 and GCS) when the data is aging out.")),(0,i.kt)("h2",{id:"contents"},"Contents"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-messaging"},"Messaging Concepts")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-architecture-overview"},"Architecture Overview")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-clients"},"Pulsar Clients")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-replication"},"Geo Replication")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-multi-tenancy"},"Multi Tenancy")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-authentication"},"Authentication and Authorization")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-topic-compaction"},"Topic Compaction")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"concepts-tiered-storage"},"Tiered Storage"))))}m.isMDXComponent=!0}}]);
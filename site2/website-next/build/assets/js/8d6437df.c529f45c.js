"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[60322],{3905:function(e,r,t){t.d(r,{Zo:function(){return u},kt:function(){return k}});var n=t(67294);function o(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function a(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);r&&(n=n.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,n)}return t}function l(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?a(Object(t),!0).forEach((function(r){o(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):a(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function c(e,r){if(null==e)return{};var t,n,o=function(e,r){if(null==e)return{};var t,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)t=a[n],r.indexOf(t)>=0||(o[t]=e[t]);return o}(e,r);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)t=a[n],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var p=n.createContext({}),i=function(e){var r=n.useContext(p),t=r;return e&&(t="function"==typeof e?e(r):l(l({},r),e)),t},u=function(e){var r=i(e.components);return n.createElement(p.Provider,{value:r},e.children)},s={inlineCode:"code",wrapper:function(e){var r=e.children;return n.createElement(n.Fragment,{},r)}},d=n.forwardRef((function(e,r){var t=e.components,o=e.mdxType,a=e.originalType,p=e.parentName,u=c(e,["components","mdxType","originalType","parentName"]),d=i(t),k=o,m=d["".concat(p,".").concat(k)]||d[k]||s[k]||a;return t?n.createElement(m,l(l({ref:r},u),{},{components:t})):n.createElement(m,l({ref:r},u))}));function k(e,r){var t=arguments,o=r&&r.mdxType;if("string"==typeof e||o){var a=t.length,l=new Array(a);l[0]=d;var c={};for(var p in r)hasOwnProperty.call(r,p)&&(c[p]=r[p]);c.originalType=e,c.mdxType="string"==typeof e?e:o,l[1]=c;for(var i=2;i<a;i++)l[i]=t[i];return n.createElement.apply(null,l)}return n.createElement.apply(null,t)}d.displayName="MDXCreateElement"},70499:function(e,r,t){t.r(r),t.d(r,{frontMatter:function(){return c},contentTitle:function(){return p},metadata:function(){return i},toc:function(){return u},default:function(){return d}});var n=t(87462),o=t(63366),a=(t(67294),t(3905)),l=["components"],c={id:"deploy-docker",title:"Deploy a cluster on Docker",sidebar_label:"Docker"},p=void 0,i={unversionedId:"deploy-docker",id:"deploy-docker",isDocsHomePage:!1,title:"Deploy a cluster on Docker",description:"To deploy a Pulsar cluster on Docker, complete the following steps:",source:"@site/docs/deploy-docker.md",sourceDirName:".",slug:"/deploy-docker",permalink:"/docs/next/deploy-docker",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/docs/deploy-docker.md",tags:[],version:"current",frontMatter:{id:"deploy-docker",title:"Deploy a cluster on Docker",sidebar_label:"Docker"},sidebar:"docsSidebar",previous:{title:"DC/OS",permalink:"/docs/next/deploy-dcos"},next:{title:"Monitor",permalink:"/docs/next/deploy-monitoring"}},u=[{value:"Prepare",id:"prepare",children:[{value:"Pull a Pulsar image",id:"pull-a-pulsar-image",children:[]},{value:"Create three containers",id:"create-three-containers",children:[]},{value:"Create a network",id:"create-a-network",children:[]},{value:"Connect containers to network",id:"connect-containers-to-network",children:[]}]}],s={toc:u};function d(e){var r=e.components,t=(0,o.Z)(e,l);return(0,a.kt)("wrapper",(0,n.Z)({},s,t,{components:r,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"To deploy a Pulsar cluster on Docker, complete the following steps:"),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},"Deploy a ZooKeeper cluster (optional)"),(0,a.kt)("li",{parentName:"ol"},"Initialize cluster metadata"),(0,a.kt)("li",{parentName:"ol"},"Deploy a BookKeeper cluster"),(0,a.kt)("li",{parentName:"ol"},"Deploy one or more Pulsar brokers")),(0,a.kt)("h2",{id:"prepare"},"Prepare"),(0,a.kt)("p",null,"To run Pulsar on Docker, you need to create a container for each Pulsar component: ZooKeeper, BookKeeper and broker. You can pull the images of ZooKeeper and BookKeeper separately on ",(0,a.kt)("a",{parentName:"p",href:"https://hub.docker.com/"},"Docker Hub"),", and pull a ",(0,a.kt)("a",{parentName:"p",href:"https://hub.docker.com/r/apachepulsar/pulsar-all/tags"},"Pulsar image")," for the broker. You can also pull only one ",(0,a.kt)("a",{parentName:"p",href:"https://hub.docker.com/r/apachepulsar/pulsar-all/tags"},"Pulsar image")," and create three containers with this image. This tutorial takes the second option as an example."),(0,a.kt)("h3",{id:"pull-a-pulsar-image"},"Pull a Pulsar image"),(0,a.kt)("p",null,"You can pull a Pulsar image from ",(0,a.kt)("a",{parentName:"p",href:"https://hub.docker.com/r/apachepulsar/pulsar-all/tags"},"Docker Hub")," with the following command."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\ndocker pull apachepulsar/pulsar-all:latest\n\n")),(0,a.kt)("h3",{id:"create-three-containers"},"Create three containers"),(0,a.kt)("p",null,"Create containers for ZooKeeper, BookKeeper and broker. In this example, they are named as ",(0,a.kt)("inlineCode",{parentName:"p"},"zookeeper"),", ",(0,a.kt)("inlineCode",{parentName:"p"},"bookkeeper")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"broker")," respectively. You can name them as you want with the ",(0,a.kt)("inlineCode",{parentName:"p"},"--name")," flag. By default, the container names are created randomly."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\ndocker run -it --name bookkeeper apachepulsar/pulsar-all:latest /bin/bash\ndocker run -it --name zookeeper apachepulsar/pulsar-all:latest /bin/bash\ndocker run -it --name broker apachepulsar/pulsar-all:latest /bin/bash\n\n")),(0,a.kt)("h3",{id:"create-a-network"},"Create a network"),(0,a.kt)("p",null,"To deploy a Pulsar cluster on Docker, you need to create a ",(0,a.kt)("inlineCode",{parentName:"p"},"network")," and connect the containers of ZooKeeper, BookKeeper and broker to this network. The following command creates the network ",(0,a.kt)("inlineCode",{parentName:"p"},"pulsar"),":"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\ndocker network create pulsar\n\n")),(0,a.kt)("h3",{id:"connect-containers-to-network"},"Connect containers to network"),(0,a.kt)("p",null,"Connect the containers of ZooKeeper, BookKeeper and broker to the ",(0,a.kt)("inlineCode",{parentName:"p"},"pulsar")," network with the following commands. "),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\ndocker network connect pulsar zookeeper\ndocker network connect pulsar bookkeeper\ndocker network connect pulsar broker\n\n")),(0,a.kt)("p",null,"To check whether the containers are successfully connected to the network, enter the ",(0,a.kt)("inlineCode",{parentName:"p"},"docker network inspect pulsar")," command."),(0,a.kt)("p",null,"For detailed information about how to deploy ZooKeeper cluster, BookKeeper cluster, brokers, see ",(0,a.kt)("a",{parentName:"p",href:"deploy-bare-metal"},"deploy a cluster on bare metal"),"."))}d.isMDXComponent=!0}}]);
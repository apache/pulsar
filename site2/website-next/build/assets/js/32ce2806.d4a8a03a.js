"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[58703],{3905:function(e,t,a){a.d(t,{Zo:function(){return p},kt:function(){return h}});var r=a(67294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},l=Object.keys(e);for(r=0;r<l.length;r++)a=l[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)a=l[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var s=r.createContext({}),u=function(e){var t=r.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},p=function(e){var t=u(e.components);return r.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,l=e.originalType,s=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),m=u(a),h=n,d=m["".concat(s,".").concat(h)]||m[h]||c[h]||l;return a?r.createElement(d,o(o({ref:t},p),{},{components:a})):r.createElement(d,o({ref:t},p))}));function h(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var l=a.length,o=new Array(l);o[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:n,o[1]=i;for(var u=2;u<l;u++)o[u]=a[u];return r.createElement.apply(null,o)}return r.createElement.apply(null,a)}m.displayName="MDXCreateElement"},21703:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return i},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return p},default:function(){return m}});var r=a(87462),n=a(63366),l=(a(67294),a(3905)),o=["components"],i={id:"helm-overview",title:"Apache Pulsar Helm Chart",sidebar_label:"Overview",original_id:"helm-overview"},s=void 0,u={unversionedId:"helm-overview",id:"version-2.6.3/helm-overview",isDocsHomePage:!1,title:"Apache Pulsar Helm Chart",description:"This is the official supported Helm chart to install Apache Pulsar on a cloud-native environment. It was enhanced based on StreamNative's Helm Chart.",source:"@site/versioned_docs/version-2.6.3/helm-overview.md",sourceDirName:".",slug:"/helm-overview",permalink:"/docs/2.6.3/helm-overview",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.3/helm-overview.md",tags:[],version:"2.6.3",frontMatter:{id:"helm-overview",title:"Apache Pulsar Helm Chart",sidebar_label:"Overview",original_id:"helm-overview"},sidebar:"version-2.6.3/docsSidebar",previous:{title:"REST APIs",permalink:"/docs/2.6.3/sql-rest-api"},next:{title:"Prepare",permalink:"/docs/2.6.3/helm-prepare"}},p=[{value:"Introduction",id:"introduction",children:[]},{value:"Pulsar Helm chart quick start",id:"pulsar-helm-chart-quick-start",children:[]},{value:"Troubleshooting",id:"troubleshooting",children:[]},{value:"Installation",id:"installation",children:[]},{value:"Upgrading",id:"upgrading",children:[]},{value:"Uninstallation",id:"uninstallation",children:[]}],c={toc:p};function m(e){var t=e.components,a=(0,n.Z)(e,o);return(0,l.kt)("wrapper",(0,r.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("p",null,"This is the official supported Helm chart to install Apache Pulsar on a cloud-native environment. It was enhanced based on StreamNative's ",(0,l.kt)("a",{parentName:"p",href:"https://github.com/streamnative/charts"},"Helm Chart"),"."),(0,l.kt)("h2",{id:"introduction"},"Introduction"),(0,l.kt)("p",null,"The Apache Pulsar Helm chart is one of the most convenient ways to operate Pulsar on Kubernetes. This Pulsar Helm chart contains all the required components to get started and can scale to large deployments."),(0,l.kt)("p",null,"This chart includes all the components for a complete experience, but each part can be configured to be installed separately."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Pulsar core components:",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"ZooKeeper"),(0,l.kt)("li",{parentName:"ul"},"Bookies"),(0,l.kt)("li",{parentName:"ul"},"Brokers"),(0,l.kt)("li",{parentName:"ul"},"Function workers"),(0,l.kt)("li",{parentName:"ul"},"Proxies"))),(0,l.kt)("li",{parentName:"ul"},"Control Center:",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"Pulsar Manager"),(0,l.kt)("li",{parentName:"ul"},"Prometheus"),(0,l.kt)("li",{parentName:"ul"},"Grafana"),(0,l.kt)("li",{parentName:"ul"},"Alert Manager")))),(0,l.kt)("p",null,"It includes support for:"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Security",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"Automatically provisioned TLS certificates, using ",(0,l.kt)("a",{parentName:"li",href:"https://www.jetstack.io/"},"Jetstack"),"'s ",(0,l.kt)("a",{parentName:"li",href:"https://cert-manager.io/docs/"},"cert-manager"),(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"self-signed"),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("a",{parentName:"li",href:"https://letsencrypt.org/"},"Let's Encrypt")))),(0,l.kt)("li",{parentName:"ul"},"TLS Encryption",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"Proxy"),(0,l.kt)("li",{parentName:"ul"},"Broker"),(0,l.kt)("li",{parentName:"ul"},"Toolset"),(0,l.kt)("li",{parentName:"ul"},"Bookie"),(0,l.kt)("li",{parentName:"ul"},"ZooKeeper"))),(0,l.kt)("li",{parentName:"ul"},"Authentication",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"JWT"))),(0,l.kt)("li",{parentName:"ul"},"Authorization"))),(0,l.kt)("li",{parentName:"ul"},"Storage",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"Non-persistence storage"),(0,l.kt)("li",{parentName:"ul"},"Persistence volume"),(0,l.kt)("li",{parentName:"ul"},"Local persistent volumes"))),(0,l.kt)("li",{parentName:"ul"},"Functions",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"Kubernetes Runtime"),(0,l.kt)("li",{parentName:"ul"},"Process Runtime"),(0,l.kt)("li",{parentName:"ul"},"Thread Runtime"))),(0,l.kt)("li",{parentName:"ul"},"Operations",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"Independent image versions for all components, enabling controlled upgrades")))),(0,l.kt)("h2",{id:"pulsar-helm-chart-quick-start"},"Pulsar Helm chart quick start"),(0,l.kt)("p",null,"To get up and run with these charts as fast as possible, in a ",(0,l.kt)("strong",{parentName:"p"},"non-production")," use case, we provide a ",(0,l.kt)("a",{parentName:"p",href:"getting-started-helm"},"quick start guide")," for Proof of Concept (PoC) deployments."),(0,l.kt)("p",null,"This guide walks the user through deploying these charts with default values and features, but ",(0,l.kt)("em",{parentName:"p"},"does not")," meet production ready requirements. To deploy these charts into production under sustained load, follow the complete ",(0,l.kt)("a",{parentName:"p",href:"helm-install"},"Installation Guide"),"."),(0,l.kt)("h2",{id:"troubleshooting"},"Troubleshooting"),(0,l.kt)("p",null,"We have done our best to make these charts as seamless as possible. Occasionally, troubles do go outside of our control. We have collected tips and tricks for troubleshooting common issues. Please check them first before raising an ",(0,l.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/issues/new/choose"},"issue"),", and feel free to add to them by raising a ",(0,l.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/compare"},"Pull Request"),"."),(0,l.kt)("h2",{id:"installation"},"Installation"),(0,l.kt)("p",null,"The Apache Pulsar Helm chart contains all required dependencies."),(0,l.kt)("p",null,"If you deploy a PoC for testing, we strongly suggest you follow our ",(0,l.kt)("a",{parentName:"p",href:"getting-started-helm"},"Quick Start Guide")," for your first iteration."),(0,l.kt)("ol",null,(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("a",{parentName:"li",href:"helm-prepare"},"Preparation")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("a",{parentName:"li",href:"helm-deploy"},"Deployment"))),(0,l.kt)("h2",{id:"upgrading"},"Upgrading"),(0,l.kt)("p",null,"Once the Pulsar Helm chart is installed, use the ",(0,l.kt)("inlineCode",{parentName:"p"},"helm upgrade")," to complete configuration changes and chart updates."),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\ngit clone https://github.com/apache/pulsar\ncd deployment/kubernetes/helm\nhelm get values <pulsar-release-name> > pulsar.yaml\nhelm upgrade <pulsar-release-name> pulsar -f pulsar.yaml\n\n")),(0,l.kt)("p",null,"For more detailed information, see ",(0,l.kt)("a",{parentName:"p",href:"helm-upgrade"},"Upgrading"),"."),(0,l.kt)("h2",{id:"uninstallation"},"Uninstallation"),(0,l.kt)("p",null,"To uninstall the Pulsar Helm chart, run the following command:"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nhelm delete <pulsar-release-name>\n\n")),(0,l.kt)("p",null,"For the purposes of continuity, these charts have some Kubernetes objects that cannot be removed when performing ",(0,l.kt)("inlineCode",{parentName:"p"},"helm delete"),".\nIt is recommended to ",(0,l.kt)("em",{parentName:"p"},"consciously")," remove these items, as they affect re-deployment."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"PVCs for stateful data: ",(0,l.kt)("em",{parentName:"li"},"consciously")," remove these items.",(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"ZooKeeper: This is your metadata."),(0,l.kt)("li",{parentName:"ul"},"BookKeeper: This is your data."),(0,l.kt)("li",{parentName:"ul"},"Prometheus: This is your metrics data, which can be safely removed."))),(0,l.kt)("li",{parentName:"ul"},"Secrets: if the secrets are generated by the ",(0,l.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/deployment/kubernetes/helm/scripts/pulsar/prepare_helm_release.sh"},"prepare release script"),", they contain secret keys and tokens. You can use the ",(0,l.kt)("a",{parentName:"li",href:"https://github.com/apache/pulsar/blob/master/deployment/kubernetes/helm/scripts/pulsar/cleanup_helm_release.sh"},"cleanup release script")," to remove these secrets and tokens as needed.")))}m.isMDXComponent=!0}}]);
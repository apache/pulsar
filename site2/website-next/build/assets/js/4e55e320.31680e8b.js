"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[24145],{3905:function(e,a,n){n.d(a,{Zo:function(){return o},kt:function(){return u}});var t=n(67294);function s(e,a,n){return a in e?Object.defineProperty(e,a,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[a]=n,e}function r(e,a){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var t=Object.getOwnPropertySymbols(e);a&&(t=t.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),n.push.apply(n,t)}return n}function i(e){for(var a=1;a<arguments.length;a++){var n=null!=arguments[a]?arguments[a]:{};a%2?r(Object(n),!0).forEach((function(a){s(e,a,n[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(n,a))}))}return e}function p(e,a){if(null==e)return{};var n,t,s=function(e,a){if(null==e)return{};var n,t,s={},r=Object.keys(e);for(t=0;t<r.length;t++)n=r[t],a.indexOf(n)>=0||(s[n]=e[n]);return s}(e,a);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(t=0;t<r.length;t++)n=r[t],a.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(s[n]=e[n])}return s}var l=t.createContext({}),c=function(e){var a=t.useContext(l),n=a;return e&&(n="function"==typeof e?e(a):i(i({},a),e)),n},o=function(e){var a=c(e.components);return t.createElement(l.Provider,{value:a},e.children)},m={inlineCode:"code",wrapper:function(e){var a=e.children;return t.createElement(t.Fragment,{},a)}},d=t.forwardRef((function(e,a){var n=e.components,s=e.mdxType,r=e.originalType,l=e.parentName,o=p(e,["components","mdxType","originalType","parentName"]),d=c(n),u=s,k=d["".concat(l,".").concat(u)]||d[u]||m[u]||r;return n?t.createElement(k,i(i({ref:a},o),{},{components:n})):t.createElement(k,i({ref:a},o))}));function u(e,a){var n=arguments,s=a&&a.mdxType;if("string"==typeof e||s){var r=n.length,i=new Array(r);i[0]=d;var p={};for(var l in a)hasOwnProperty.call(a,l)&&(p[l]=a[l]);p.originalType=e,p.mdxType="string"==typeof e?e:s,i[1]=p;for(var c=2;c<r;c++)i[c]=n[c];return t.createElement.apply(null,i)}return t.createElement.apply(null,n)}d.displayName="MDXCreateElement"},58215:function(e,a,n){var t=n(67294);a.Z=function(e){var a=e.children,n=e.hidden,s=e.className;return t.createElement("div",{role:"tabpanel",hidden:n,className:s},a)}},55064:function(e,a,n){n.d(a,{Z:function(){return c}});var t=n(67294),s=n(79443);var r=function(){var e=(0,t.useContext)(s.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},i=n(86010),p="tabItem_1uMI",l="tabItemActive_2DSg";var c=function(e){var a,n=e.lazy,s=e.block,c=e.defaultValue,o=e.values,m=e.groupId,d=e.className,u=t.Children.toArray(e.children),k=null!=o?o:u.map((function(e){return{value:e.props.value,label:e.props.label}})),h=null!=c?c:null==(a=u.find((function(e){return e.props.default})))?void 0:a.props.value,g=r(),f=g.tabGroupChoices,v=g.setTabGroupChoices,N=(0,t.useState)(h),b=N[0],T=N[1],y=[];if(null!=m){var j=f[m];null!=j&&j!==b&&k.some((function(e){return e.value===j}))&&T(j)}var R=function(e){var a=e.currentTarget,n=y.indexOf(a),t=k[n].value;T(t),null!=m&&(v(m,t),setTimeout((function(){var e,n,t,s,r,i,p,c;(e=a.getBoundingClientRect(),n=e.top,t=e.left,s=e.bottom,r=e.right,i=window,p=i.innerHeight,c=i.innerWidth,n>=0&&r<=c&&s<=p&&t>=0)||(a.scrollIntoView({block:"center",behavior:"smooth"}),a.classList.add(l),setTimeout((function(){return a.classList.remove(l)}),2e3))}),150))},C=function(e){var a,n=null;switch(e.key){case"ArrowRight":var t=y.indexOf(e.target)+1;n=y[t]||y[0];break;case"ArrowLeft":var s=y.indexOf(e.target)-1;n=y[s]||y[y.length-1]}null==(a=n)||a.focus()};return t.createElement("div",{className:"tabs-container"},t.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":s},d)},k.map((function(e){var a=e.value,n=e.label;return t.createElement("li",{role:"tab",tabIndex:b===a?0:-1,"aria-selected":b===a,className:(0,i.Z)("tabs__item",p,{"tabs__item--active":b===a}),key:a,ref:function(e){return y.push(e)},onKeyDown:C,onFocus:R,onClick:R},null!=n?n:a)}))),n?(0,t.cloneElement)(u.filter((function(e){return e.props.value===b}))[0],{className:"margin-vert--md"}):t.createElement("div",{className:"margin-vert--md"},u.map((function(e,a){return(0,t.cloneElement)(e,{key:a,hidden:e.props.value!==b})}))))}},79443:function(e,a,n){var t=(0,n(67294).createContext)(void 0);a.Z=t},4047:function(e,a,n){n.r(a),n.d(a,{frontMatter:function(){return c},contentTitle:function(){return o},metadata:function(){return m},toc:function(){return d},default:function(){return k}});var t=n(87462),s=n(63366),r=(n(67294),n(3905)),i=n(55064),p=n(58215),l=["components"],c={id:"admin-api-namespaces",title:"Managing Namespaces",sidebar_label:"Namespaces",original_id:"admin-api-namespaces"},o=void 0,m={unversionedId:"admin-api-namespaces",id:"version-2.6.2/admin-api-namespaces",isDocsHomePage:!1,title:"Managing Namespaces",description:"Pulsar namespaces are logical groupings of topics.",source:"@site/versioned_docs/version-2.6.2/admin-api-namespaces.md",sourceDirName:".",slug:"/admin-api-namespaces",permalink:"/docs/2.6.2/admin-api-namespaces",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.2/admin-api-namespaces.md",tags:[],version:"2.6.2",frontMatter:{id:"admin-api-namespaces",title:"Managing Namespaces",sidebar_label:"Namespaces",original_id:"admin-api-namespaces"},sidebar:"version-2.6.2/docsSidebar",previous:{title:"Brokers",permalink:"/docs/2.6.2/admin-api-brokers"},next:{title:"Permissions",permalink:"/docs/2.6.2/admin-api-permissions"}},d=[{value:"Namespaces resources",id:"namespaces-resources",children:[{value:"Create",id:"create",children:[]},{value:"Get policies",id:"get-policies",children:[]},{value:"List namespaces within a tenant",id:"list-namespaces-within-a-tenant",children:[]},{value:"Delete",id:"delete",children:[]},{value:"Namespace isolation",id:"namespace-isolation",children:[]},{value:"Unloading from a broker",id:"unloading-from-a-broker",children:[]}]}],u={toc:d};function k(e){var a=e.components,n=(0,s.Z)(e,l);return(0,r.kt)("wrapper",(0,t.Z)({},u,n,{components:a,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"Pulsar ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#namespace"},"namespaces")," are logical groupings of ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#topic"},"topics"),"."),(0,r.kt)("p",null,"Namespaces can be managed via:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("a",{parentName:"li",href:"/docs/2.6.2/pulsar-admin#clusters"},(0,r.kt)("inlineCode",{parentName:"a"},"namespaces"))," command of the ",(0,r.kt)("a",{parentName:"li",href:"reference-pulsar-admin"},(0,r.kt)("inlineCode",{parentName:"a"},"pulsar-admin"))," tool"),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"/admin/v2/namespaces")," endpoint of the admin ",(0,r.kt)("a",{parentName:"li",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"REST")," API"),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"namespaces")," method of the ",(0,r.kt)("a",{parentName:"li",href:"https://pulsar.incubator.apache.org/api/admin/org/apache/pulsar/client/admin/PulsarAdmin"},"PulsarAdmin")," object in the ",(0,r.kt)("a",{parentName:"li",href:"client-libraries-java"},"Java API"))),(0,r.kt)("h2",{id:"namespaces-resources"},"Namespaces resources"),(0,r.kt)("h3",{id:"create"},"Create"),(0,r.kt)("p",null,"You can create new namespaces under a given ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#tenant"},"tenant"),"."),(0,r.kt)("h4",{id:"pulsar-admin"},"pulsar-admin"),(0,r.kt)("p",null,"Use the ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#namespaces-create"},(0,r.kt)("inlineCode",{parentName:"a"},"create"))," subcommand and specify the namespace by name:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin namespaces create test-tenant/test-namespace\n\n")),(0,r.kt)("h4",{id:"rest-api"},"REST API"),(0,r.kt)("p",null,(0,r.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/createNamespace?version=2.6.2&apiVersion=v2"},"PUT /admin/v2/namespaces/:tenant/:namespace")),(0,r.kt)("h4",{id:"java"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().createNamespace(namespace);\n\n")),(0,r.kt)("h3",{id:"get-policies"},"Get policies"),(0,r.kt)("p",null,"You can fetch the current policies associated with a namespace at any time."),(0,r.kt)("h4",{id:"pulsar-admin-1"},"pulsar-admin"),(0,r.kt)("p",null,"Use the ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#namespaces-policies"},(0,r.kt)("inlineCode",{parentName:"a"},"policies"))," subcommand and specify the namespace:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},'\n$ pulsar-admin namespaces policies test-tenant/test-namespace\n{\n  "auth_policies": {\n    "namespace_auth": {},\n    "destination_auth": {}\n  },\n  "replication_clusters": [],\n  "bundles_activated": true,\n  "bundles": {\n    "boundaries": [\n      "0x00000000",\n      "0xffffffff"\n    ],\n    "numBundles": 1\n  },\n  "backlog_quota_map": {},\n  "persistence": null,\n  "latency_stats_sample_rate": {},\n  "message_ttl_in_seconds": 0,\n  "retention_policies": null,\n  "deleted": false\n}\n\n')),(0,r.kt)("h4",{id:"rest-api-1"},"REST API"),(0,r.kt)("p",null,(0,r.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getPolicies?version=2.6.2&apiVersion=v2"},"GET /admin/v2/namespaces/:tenant/:namespace")),(0,r.kt)("h4",{id:"java-1"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getPolicies(namespace);\n\n")),(0,r.kt)("h3",{id:"list-namespaces-within-a-tenant"},"List namespaces within a tenant"),(0,r.kt)("p",null,"You can list all namespaces within a given Pulsar ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#tenant"},"tenant"),"."),(0,r.kt)("h4",{id:"pulsar-admin-2"},"pulsar-admin"),(0,r.kt)("p",null,"Use the ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#namespaces-list"},(0,r.kt)("inlineCode",{parentName:"a"},"list"))," subcommand and specify the tenant:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin namespaces list test-tenant\ntest-tenant/ns1\ntest-tenant/ns2\n\n")),(0,r.kt)("h4",{id:"rest-api-2"},"REST API"),(0,r.kt)("p",null,(0,r.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getTenantNamespaces?version=2.6.2&apiVersion=v2"},"GET /admin/v2/namespaces/:tenant")),(0,r.kt)("h4",{id:"java-2"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getNamespaces(tenant);\n\n")),(0,r.kt)("h3",{id:"delete"},"Delete"),(0,r.kt)("p",null,"You can delete existing namespaces from a tenant."),(0,r.kt)("h4",{id:"pulsar-admin-3"},"pulsar-admin"),(0,r.kt)("p",null,"Use the ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#namespaces-delete"},(0,r.kt)("inlineCode",{parentName:"a"},"delete"))," subcommand and specify the namespace:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin namespaces delete test-tenant/ns1\n\n")),(0,r.kt)("h4",{id:"rest"},"REST"),(0,r.kt)("p",null,(0,r.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/deleteNamespace?version=2.6.2&apiVersion=v2"},"DELETE /admin/v2/namespaces/:tenant/:namespace")),(0,r.kt)("h4",{id:"java-3"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().deleteNamespace(namespace);\n\n")),(0,r.kt)("h4",{id:"set-replication-cluster"},"set replication cluster"),(0,r.kt)("p",null,"It sets replication clusters for a namespace, so Pulsar can internally replicate publish message from one colo to another colo."),(0,r.kt)("h6",{id:"cli"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces set-clusters test-tenant/ns1 \\\n  --clusters cl1\n\n")),(0,r.kt)("h6",{id:"rest-1"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/replication|operation/setNamespaceReplicationClusters?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-4"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setNamespaceReplicationClusters(namespace, clusters);\n\n")),(0,r.kt)("h4",{id:"get-replication-cluster"},"get replication cluster"),(0,r.kt)("p",null,"It gives a list of replication clusters for a given namespace."),(0,r.kt)("h6",{id:"cli-1"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-clusters test-tenant/cl1/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\ncl2\n\n")),(0,r.kt)("h6",{id:"rest-2"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/replication|operation/getNamespaceReplicationClusters?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-5"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getNamespaceReplicationClusters(namespace)\n\n")),(0,r.kt)("h4",{id:"set-backlog-quota-policies"},"set backlog quota policies"),(0,r.kt)("p",null,"Backlog quota helps broker to restrict bandwidth/storage of a namespace once it reach certain threshold limit . Admin can set this limit and one of the following action after the limit is reached."),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"producer_request_hold: broker will hold and not persist produce request payload")),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"producer_exception: broker will disconnects with client by giving exception")),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"consumer_backlog_eviction: broker will start discarding backlog messages"),(0,r.kt)("p",{parentName:"li"},"Backlog quota restriction can be taken care by defining restriction of backlog-quota-type: destination_storage"))),(0,r.kt)("h6",{id:"cli-2"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces set-backlog-quota --limit 10 --policy producer_request_hold test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-3"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/setBacklogQuota?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-6"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, policy))\n\n")),(0,r.kt)("h4",{id:"get-backlog-quota-policies"},"get backlog quota policies"),(0,r.kt)("p",null,"It shows a configured backlog quota for a given namespace."),(0,r.kt)("h6",{id:"cli-3"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-backlog-quotas test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "destination_storage": {\n    "limit": 10,\n    "policy": "producer_request_hold"\n  }\n}\n\n')),(0,r.kt)("h6",{id:"rest-4"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/backlogQuotaMap|operation/getBacklogQuotaMap?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-7"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getBacklogQuotaMap(namespace);\n\n")),(0,r.kt)("h4",{id:"remove-backlog-quota-policies"},"remove backlog quota policies"),(0,r.kt)("p",null,"It removes backlog quota policies for a given namespace"),(0,r.kt)("h6",{id:"cli-4"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces remove-backlog-quota test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-5"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/removeBacklogQuota?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-8"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().removeBacklogQuota(namespace, backlogQuotaType)\n\n")),(0,r.kt)("h4",{id:"set-persistence-policies"},"set persistence policies"),(0,r.kt)("p",null,"Persistence policies allow to configure persistency-level for all topic messages under a given namespace."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Bookkeeper-ack-quorum: Number of acks (guaranteed copies) to wait for each entry, default: 0")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Bookkeeper-ensemble: Number of bookies to use for a topic, default: 0")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Bookkeeper-write-quorum: How many writes to make of each entry, default: 0")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Ml-mark-delete-max-rate: Throttling rate of mark-delete operation (0 means no throttle), default: 0.0"))),(0,r.kt)("h6",{id:"cli-5"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces set-persistence --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-6"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/setPersistence?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-9"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setPersistence(namespace,new PersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum,bookkeeperAckQuorum,managedLedgerMaxMarkDeleteRate))\n\n")),(0,r.kt)("h4",{id:"get-persistence-policies"},"get persistence policies"),(0,r.kt)("p",null,"It shows configured persistence policies of a given namespace."),(0,r.kt)("h6",{id:"cli-6"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-persistence test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "bookkeeperEnsemble": 3,\n  "bookkeeperWriteQuorum": 2,\n  "bookkeeperAckQuorum": 2,\n  "managedLedgerMaxMarkDeleteRate": 0\n}\n\n')),(0,r.kt)("h6",{id:"rest-7"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/getPersistence?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-10"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getPersistence(namespace)\n\n")),(0,r.kt)("h4",{id:"unload-namespace-bundle"},"unload namespace bundle"),(0,r.kt)("p",null,"Namespace bundle is a virtual group of topics which belong to same namespace. If broker gets overloaded with number of bundles then this command can help to unload heavy bundle from that broker, so it can be served by some other less loaded broker. Namespace bundle is defined with it\u2019s start and end range such as 0x00000000 and 0xffffffff."),(0,r.kt)("h6",{id:"cli-7"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-8"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/{bundle}/unload|operation/unloadNamespaceBundle?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-11"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().unloadNamespaceBundle(namespace, bundle)\n\n")),(0,r.kt)("h4",{id:"set-message-ttl"},"set message-ttl"),(0,r.kt)("p",null,"It configures message\u2019s time to live (in seconds) duration."),(0,r.kt)("h6",{id:"cli-8"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces set-message-ttl --messageTTL 100 test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-9"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/setNamespaceMessageTTL?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-12"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setNamespaceMessageTTL(namespace, messageTTL)\n\n")),(0,r.kt)("h4",{id:"get-message-ttl"},"get message-ttl"),(0,r.kt)("p",null,"It gives a message ttl of configured namespace."),(0,r.kt)("h6",{id:"cli-9"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-message-ttl test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n100\n\n")),(0,r.kt)("h6",{id:"rest-10"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/getNamespaceMessageTTL?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-13"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getNamespaceMessageTTL(namespace)\n\n")),(0,r.kt)("h4",{id:"remove-message-ttl"},"Remove message-ttl"),(0,r.kt)("p",null,"Remove a message TTL of the configured namespace."),(0,r.kt)(i.Z,{defaultValue:"pulsar-admin",values:[{label:"pulsar-admin",value:"pulsar-admin"},{label:"REST API",value:"REST API"},{label:"Java",value:"Java"}],mdxType:"Tabs"},(0,r.kt)(p.Z,{value:"pulsar-admin",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces remove-message-ttl test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n100\n\n"))),(0,r.kt)(p.Z,{value:"REST API",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/removeNamespaceMessageTTL?version=2.6.2}\n\n"))),(0,r.kt)(p.Z,{value:"Java",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().removeNamespaceMessageTTL(namespace)\n\n")))),(0,r.kt)("h4",{id:"split-bundle"},"split bundle"),(0,r.kt)("p",null,"Each namespace bundle can contain multiple topics and each bundle can be served by only one broker. If bundle gets heavy with multiple live topics in it then it creates load on that broker and in order to resolve this issue, admin can split bundle using this command."),(0,r.kt)("h6",{id:"cli-10"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-11"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/{bundle}/split|operation/splitNamespaceBundle?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-14"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().splitNamespaceBundle(namespace, bundle)\n\n")),(0,r.kt)("h4",{id:"clear-backlog"},"clear backlog"),(0,r.kt)("p",null,"It clears all message backlog for all the topics those belong to specific namespace. You can also clear backlog for a specific subscription as well."),(0,r.kt)("h6",{id:"cli-11"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces clear-backlog --sub my-subscription test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-12"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/clearBacklog|operation/clearNamespaceBacklogForSubscription?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-15"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription)\n\n")),(0,r.kt)("h4",{id:"clear-bundle-backlog"},"clear bundle backlog"),(0,r.kt)("p",null,"It clears all message backlog for all the topics those belong to specific NamespaceBundle. You can also clear backlog for a specific subscription as well."),(0,r.kt)("h6",{id:"cli-12"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces clear-backlog  --bundle 0x00000000_0xffffffff  --sub my-subscription test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-13"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/{bundle}/clearBacklog|operation/clearNamespaceBundleBacklogForSubscription?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-16"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription)\n\n")),(0,r.kt)("h4",{id:"set-retention"},"set retention"),(0,r.kt)("p",null,"Each namespace contains multiple topics and each topic\u2019s retention size (storage size) should not exceed to a specific threshold or it should be stored till certain time duration. This command helps to configure retention size and time of topics in a given namespace."),(0,r.kt)("h6",{id:"cli-13"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin set-retention --size 10 --time 100 test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\nN/A\n\n")),(0,r.kt)("h6",{id:"rest-14"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/retention|operation/setRetention?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-17"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB))\n\n")),(0,r.kt)("h4",{id:"get-retention"},"get retention"),(0,r.kt)("p",null,"It shows retention information of a given namespace."),(0,r.kt)("h6",{id:"cli-14"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-retention test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "retentionTimeInMinutes": 10,\n  "retentionSizeInMB": 100\n}\n\n')),(0,r.kt)("h6",{id:"rest-15"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/retention|operation/getRetention?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-18"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getRetention(namespace)\n\n")),(0,r.kt)("h4",{id:"set-dispatch-throttling"},"set dispatch throttling"),(0,r.kt)("p",null,"It sets message dispatch rate for all the topics under a given namespace.\nDispatch rate can be restricted by number of message per X seconds (",(0,r.kt)("inlineCode",{parentName:"p"},"msg-dispatch-rate"),") or by number of message-bytes per X second (",(0,r.kt)("inlineCode",{parentName:"p"},"byte-dispatch-rate"),").\ndispatch rate is in second and it can be configured with ",(0,r.kt)("inlineCode",{parentName:"p"},"dispatch-rate-period"),". Default value of ",(0,r.kt)("inlineCode",{parentName:"p"},"msg-dispatch-rate")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"byte-dispatch-rate")," is -1 which\ndisables the throttling."),(0,r.kt)("h4",{id:"note"},"Note"),(0,r.kt)("blockquote",null,(0,r.kt)("ul",{parentName:"blockquote"},(0,r.kt)("li",{parentName:"ul"},"If neither ",(0,r.kt)("inlineCode",{parentName:"li"},"clusterDispatchRate")," nor ",(0,r.kt)("inlineCode",{parentName:"li"},"topicDispatchRate")," is configured, dispatch throttling is disabled.",(0,r.kt)("blockquote",{parentName:"li"})),(0,r.kt)("li",{parentName:"ul"},"If ",(0,r.kt)("inlineCode",{parentName:"li"},"topicDispatchRate")," is not configured, ",(0,r.kt)("inlineCode",{parentName:"li"},"clusterDispatchRate")," takes effect.",(0,r.kt)("blockquote",{parentName:"li"})),(0,r.kt)("li",{parentName:"ul"},"If ",(0,r.kt)("inlineCode",{parentName:"li"},"topicDispatchRate")," is configured, ",(0,r.kt)("inlineCode",{parentName:"li"},"topicDispatchRate")," takes effect."))),(0,r.kt)("h6",{id:"cli-15"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces set-dispatch-rate test-tenant/ns1 \\\n  --msg-dispatch-rate 1000 \\\n  --byte-dispatch-rate 1048576 \\\n  --dispatch-rate-period 1\n\n")),(0,r.kt)("h6",{id:"rest-16"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/dispatchRate|operation/setDispatchRate?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-19"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))\n\n")),(0,r.kt)("h4",{id:"get-configured-message-rate"},"get configured message-rate"),(0,r.kt)("p",null,"It shows configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)"),(0,r.kt)("h6",{id:"cli-16"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-dispatch-rate test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "dispatchThrottlingRatePerTopicInMsg" : 1000,\n  "dispatchThrottlingRatePerTopicInByte" : 1048576,\n  "ratePeriodInSecond" : 1\n}\n\n')),(0,r.kt)("h6",{id:"rest-17"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/dispatchRate|operation/getDispatchRate?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-20"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getDispatchRate(namespace)\n\n")),(0,r.kt)("h4",{id:"set-dispatch-throttling-for-subscription"},"set dispatch throttling for subscription"),(0,r.kt)("p",null,"It sets message dispatch rate for all the subscription of topics under a given namespace.\nDispatch rate can be restricted by number of message per X seconds (",(0,r.kt)("inlineCode",{parentName:"p"},"msg-dispatch-rate"),") or by number of message-bytes per X second (",(0,r.kt)("inlineCode",{parentName:"p"},"byte-dispatch-rate"),").\ndispatch rate is in second and it can be configured with ",(0,r.kt)("inlineCode",{parentName:"p"},"dispatch-rate-period"),". Default value of ",(0,r.kt)("inlineCode",{parentName:"p"},"msg-dispatch-rate")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"byte-dispatch-rate")," is -1 which\ndisables the throttling."),(0,r.kt)("h6",{id:"cli-17"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces set-subscription-dispatch-rate test-tenant/ns1 \\\n  --msg-dispatch-rate 1000 \\\n  --byte-dispatch-rate 1048576 \\\n  --dispatch-rate-period 1\n\n")),(0,r.kt)("h6",{id:"rest-18"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/subscriptionDispatchRate|operation/setDispatchRate?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-21"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setSubscriptionDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))\n\n")),(0,r.kt)("h4",{id:"get-configured-message-rate-1"},"get configured message-rate"),(0,r.kt)("p",null,"It shows configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)"),(0,r.kt)("h6",{id:"cli-18"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-subscription-dispatch-rate test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "dispatchThrottlingRatePerTopicInMsg" : 1000,\n  "dispatchThrottlingRatePerTopicInByte" : 1048576,\n  "ratePeriodInSecond" : 1\n}\n\n')),(0,r.kt)("h6",{id:"rest-19"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/subscriptionDispatchRate|operation/getDispatchRate?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-22"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getSubscriptionDispatchRate(namespace)\n\n")),(0,r.kt)("h4",{id:"set-dispatch-throttling-for-replicator"},"set dispatch throttling for replicator"),(0,r.kt)("p",null,"It sets message dispatch rate for all the replicator between replication clusters under a given namespace.\nDispatch rate can be restricted by number of message per X seconds (",(0,r.kt)("inlineCode",{parentName:"p"},"msg-dispatch-rate"),") or by number of message-bytes per X second (",(0,r.kt)("inlineCode",{parentName:"p"},"byte-dispatch-rate"),").\ndispatch rate is in second and it can be configured with ",(0,r.kt)("inlineCode",{parentName:"p"},"dispatch-rate-period"),". Default value of ",(0,r.kt)("inlineCode",{parentName:"p"},"msg-dispatch-rate")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"byte-dispatch-rate")," is -1 which\ndisables the throttling."),(0,r.kt)("h6",{id:"cli-19"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces set-replicator-dispatch-rate test-tenant/ns1 \\\n  --msg-dispatch-rate 1000 \\\n  --byte-dispatch-rate 1048576 \\\n  --dispatch-rate-period 1\n\n")),(0,r.kt)("h6",{id:"rest-20"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/replicatorDispatchRate|operation/setDispatchRate?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-23"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().setReplicatorDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))\n\n")),(0,r.kt)("h4",{id:"get-configured-message-rate-2"},"get configured message-rate"),(0,r.kt)("p",null,"It shows configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)"),(0,r.kt)("h6",{id:"cli-20"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n$ pulsar-admin namespaces get-replicator-dispatch-rate test-tenant/ns1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "dispatchThrottlingRatePerTopicInMsg" : 1000,\n  "dispatchThrottlingRatePerTopicInByte" : 1048576,\n  "ratePeriodInSecond" : 1\n}\n\n')),(0,r.kt)("h6",{id:"rest-21"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/replicatorDispatchRate|operation/getDispatchRate?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-24"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().getReplicatorDispatchRate(namespace)\n\n")),(0,r.kt)("h3",{id:"namespace-isolation"},"Namespace isolation"),(0,r.kt)("p",null,"Coming soon."),(0,r.kt)("h3",{id:"unloading-from-a-broker"},"Unloading from a broker"),(0,r.kt)("p",null,"You can unload a namespace, or a ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#namespace-bundle"},"namespace bundle"),", from the Pulsar ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#broker"},"broker")," that is currently responsible for it."),(0,r.kt)("h4",{id:"pulsar-admin-4"},"pulsar-admin"),(0,r.kt)("p",null,"Use the ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#unload"},(0,r.kt)("inlineCode",{parentName:"a"},"unload"))," subcommand of the ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#namespaces"},(0,r.kt)("inlineCode",{parentName:"a"},"namespaces"))," command."),(0,r.kt)("h6",{id:"cli-21"},"CLI"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin namespaces unload my-tenant/my-ns\n\n")),(0,r.kt)("h6",{id:"rest-22"},"REST"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/unload|operation/unloadNamespace?version=2.6.2}\n\n")),(0,r.kt)("h6",{id:"java-25"},"Java"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.namespaces().unload(namespace)\n\n")))}k.isMDXComponent=!0},86010:function(e,a,n){function t(e){var a,n,s="";if("string"==typeof e||"number"==typeof e)s+=e;else if("object"==typeof e)if(Array.isArray(e))for(a=0;a<e.length;a++)e[a]&&(n=t(e[a]))&&(s&&(s+=" "),s+=n);else for(a in e)e[a]&&(s&&(s+=" "),s+=a);return s}function s(){for(var e,a,n=0,s="";n<arguments.length;)(e=arguments[n++])&&(a=t(e))&&(s&&(s+=" "),s+=a);return s}n.d(a,{Z:function(){return s}})}}]);
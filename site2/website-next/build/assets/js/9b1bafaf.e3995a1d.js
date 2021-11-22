"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[96195],{3905:function(e,r,t){t.d(r,{Zo:function(){return p},kt:function(){return g}});var a=t(67294);function o(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function n(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);r&&(a=a.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,a)}return t}function l(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?n(Object(t),!0).forEach((function(r){o(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):n(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function i(e,r){if(null==e)return{};var t,a,o=function(e,r){if(null==e)return{};var t,a,o={},n=Object.keys(e);for(a=0;a<n.length;a++)t=n[a],r.indexOf(t)>=0||(o[t]=e[t]);return o}(e,r);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(a=0;a<n.length;a++)t=n[a],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var s=a.createContext({}),u=function(e){var r=a.useContext(s),t=r;return e&&(t="function"==typeof e?e(r):l(l({},r),e)),t},p=function(e){var r=u(e.components);return a.createElement(s.Provider,{value:r},e.children)},d={inlineCode:"code",wrapper:function(e){var r=e.children;return a.createElement(a.Fragment,{},r)}},c=a.forwardRef((function(e,r){var t=e.components,o=e.mdxType,n=e.originalType,s=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),c=u(t),g=o,k=c["".concat(s,".").concat(g)]||c[g]||d[g]||n;return t?a.createElement(k,l(l({ref:r},p),{},{components:t})):a.createElement(k,l({ref:r},p))}));function g(e,r){var t=arguments,o=r&&r.mdxType;if("string"==typeof e||o){var n=t.length,l=new Array(n);l[0]=c;var i={};for(var s in r)hasOwnProperty.call(r,s)&&(i[s]=r[s]);i.originalType=e,i.mdxType="string"==typeof e?e:o,l[1]=i;for(var u=2;u<n;u++)l[u]=t[u];return a.createElement.apply(null,l)}return a.createElement.apply(null,t)}c.displayName="MDXCreateElement"},58215:function(e,r,t){var a=t(67294);r.Z=function(e){var r=e.children,t=e.hidden,o=e.className;return a.createElement("div",{role:"tabpanel",hidden:t,className:o},r)}},55064:function(e,r,t){t.d(r,{Z:function(){return u}});var a=t(67294),o=t(79443);var n=function(){var e=(0,a.useContext)(o.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},l=t(86010),i="tabItem_1uMI",s="tabItemActive_2DSg";var u=function(e){var r,t=e.lazy,o=e.block,u=e.defaultValue,p=e.values,d=e.groupId,c=e.className,g=a.Children.toArray(e.children),k=null!=p?p:g.map((function(e){return{value:e.props.value,label:e.props.label}})),m=null!=u?u:null==(r=g.find((function(e){return e.props.default})))?void 0:r.props.value,h=n(),f=h.tabGroupChoices,b=h.setTabGroupChoices,y=(0,a.useState)(m),v=y[0],N=y[1],w=[];if(null!=d){var x=f[d];null!=x&&x!==v&&k.some((function(e){return e.value===x}))&&N(x)}var C=function(e){var r=e.currentTarget,t=w.indexOf(r),a=k[t].value;N(a),null!=d&&(b(d,a),setTimeout((function(){var e,t,a,o,n,l,i,u;(e=r.getBoundingClientRect(),t=e.top,a=e.left,o=e.bottom,n=e.right,l=window,i=l.innerHeight,u=l.innerWidth,t>=0&&n<=u&&o<=i&&a>=0)||(r.scrollIntoView({block:"center",behavior:"smooth"}),r.classList.add(s),setTimeout((function(){return r.classList.remove(s)}),2e3))}),150))},U=function(e){var r,t=null;switch(e.key){case"ArrowRight":var a=w.indexOf(e.target)+1;t=w[a]||w[0];break;case"ArrowLeft":var o=w.indexOf(e.target)-1;t=w[o]||w[w.length-1]}null==(r=t)||r.focus()};return a.createElement("div",{className:"tabs-container"},a.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":o},c)},k.map((function(e){var r=e.value,t=e.label;return a.createElement("li",{role:"tab",tabIndex:v===r?0:-1,"aria-selected":v===r,className:(0,l.Z)("tabs__item",i,{"tabs__item--active":v===r}),key:r,ref:function(e){return w.push(e)},onKeyDown:U,onFocus:C,onClick:C},null!=t?t:r)}))),t?(0,a.cloneElement)(g.filter((function(e){return e.props.value===v}))[0],{className:"margin-vert--md"}):a.createElement("div",{className:"margin-vert--md"},g.map((function(e,r){return(0,a.cloneElement)(e,{key:r,hidden:e.props.value!==v})}))))}},79443:function(e,r,t){var a=(0,t(67294).createContext)(void 0);r.Z=a},29841:function(e,r,t){t.r(r),t.d(r,{frontMatter:function(){return i},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return p},default:function(){return c}});var a=t(87462),o=t(63366),n=(t(67294),t(3905)),l=(t(55064),t(58215),["components"]),i={id:"administration-upgrade",title:"Upgrade Guide",sidebar_label:"Upgrade",original_id:"administration-upgrade"},s=void 0,u={unversionedId:"administration-upgrade",id:"version-2.7.0/administration-upgrade",isDocsHomePage:!1,title:"Upgrade Guide",description:"Upgrade guidelines",source:"@site/versioned_docs/version-2.7.0/administration-upgrade.md",sourceDirName:".",slug:"/administration-upgrade",permalink:"/docs/2.7.0/administration-upgrade",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.0/administration-upgrade.md",tags:[],version:"2.7.0",frontMatter:{id:"administration-upgrade",title:"Upgrade Guide",sidebar_label:"Upgrade",original_id:"administration-upgrade"},sidebar:"version-2.7.0/docsSidebar",previous:{title:"Pulsar proxy",permalink:"/docs/2.7.0/administration-proxy"},next:{title:"Overview",permalink:"/docs/2.7.0/security-overview"}},p=[{value:"Upgrade guidelines",id:"upgrade-guidelines",children:[]},{value:"Upgrade sequence",id:"upgrade-sequence",children:[]},{value:"Upgrade ZooKeeper (optional)",id:"upgrade-zookeeper-optional",children:[{value:"Canary test",id:"canary-test",children:[]},{value:"Upgrade all ZooKeeper servers",id:"upgrade-all-zookeeper-servers",children:[]}]},{value:"Upgrade bookies",id:"upgrade-bookies",children:[{value:"Canary test",id:"canary-test-1",children:[]},{value:"Upgrade all bookies",id:"upgrade-all-bookies",children:[]}]},{value:"Upgrade brokers and proxies",id:"upgrade-brokers-and-proxies",children:[{value:"Canary test",id:"canary-test-2",children:[]},{value:"Upgrade all brokers or proxies",id:"upgrade-all-brokers-or-proxies",children:[]}]}],d={toc:p};function c(e){var r=e.components,t=(0,o.Z)(e,l);return(0,n.kt)("wrapper",(0,a.Z)({},d,t,{components:r,mdxType:"MDXLayout"}),(0,n.kt)("h2",{id:"upgrade-guidelines"},"Upgrade guidelines"),(0,n.kt)("p",null,"Apache Pulsar is comprised of multiple components, ZooKeeper, bookies, and brokers. These components are either stateful or stateless. You do not have to upgrade ZooKeeper nodes unless you have special requirement. While you upgrade, you need to pay attention to bookies (stateful), brokers and proxies (stateless)."),(0,n.kt)("p",null,"The following are some guidelines on upgrading a Pulsar cluster. Read the guidelines before upgrading."),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Backup all your configuration files before upgrading."),(0,n.kt)("li",{parentName:"ul"},"Read guide entirely, make a plan, and then execute the plan. When you make upgrade plan, you need to take your specific requirements and environment into consideration.   "),(0,n.kt)("li",{parentName:"ul"},"Pay attention to the upgrading order of components. In general, you do not need to upgrade your ZooKeeper or configuration store cluster (the global ZooKeeper cluster). You need to upgrade bookies first, and then upgrade brokers, proxies, and your clients. "),(0,n.kt)("li",{parentName:"ul"},"If ",(0,n.kt)("inlineCode",{parentName:"li"},"autorecovery")," is enabled, you need to disable ",(0,n.kt)("inlineCode",{parentName:"li"},"autorecovery")," in the upgrade process, and re-enable it after completing the process."),(0,n.kt)("li",{parentName:"ul"},"Read the release notes carefully for each release. Release notes contain features, configuration changes that might impact your upgrade."),(0,n.kt)("li",{parentName:"ul"},"Upgrade a small subset of nodes of each type to canary test the new version before upgrading all nodes of that type in the cluster. When you have upgraded the canary nodes, run for a while to ensure that they work correctly."),(0,n.kt)("li",{parentName:"ul"},"Upgrade one data center to verify new version before upgrading all data centers if your cluster runs in multi-cluster replicated mode.")),(0,n.kt)("blockquote",null,(0,n.kt)("p",{parentName:"blockquote"},"Note: Currently, Apache Pulsar is compatible between versions. ")),(0,n.kt)("h2",{id:"upgrade-sequence"},"Upgrade sequence"),(0,n.kt)("p",null,"To upgrade an Apache Pulsar cluster, follow the upgrade sequence."),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},"Upgrade ZooKeeper (optional)  ")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Canary test: test an upgraded version in one or a small set of ZooKeeper nodes.  "),(0,n.kt)("li",{parentName:"ul"},"Rolling upgrade: rollout the upgraded version to all ZooKeeper servers incrementally, one at a time. Monitor your dashboard during the whole rolling upgrade process.")),(0,n.kt)("ol",{start:2},(0,n.kt)("li",{parentName:"ol"},"Upgrade bookies  ")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Canary test: test an upgraded version in one or a small set of bookies.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Rolling upgrade:  "),(0,n.kt)("ul",{parentName:"li"},(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"a. Disable ",(0,n.kt)("inlineCode",{parentName:"p"},"autorecovery")," with the following command."),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"\nbin/bookkeeper shell autorecovery -disable\n\n")))),(0,n.kt)("ul",{parentName:"li"},(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"b. Rollout the upgraded version to all bookies in the cluster after you determine that a version is safe after canary.  ")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"c. After you upgrade all bookies, re-enable ",(0,n.kt)("inlineCode",{parentName:"p"},"autorecovery")," with the following command."),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"\nbin/bookkeeper shell autorecovery -enable\n\n")))))),(0,n.kt)("ol",{start:3},(0,n.kt)("li",{parentName:"ol"},"Upgrade brokers")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Canary test: test an upgraded version in one or a small set of brokers."),(0,n.kt)("li",{parentName:"ul"},"Rolling upgrade: rollout the upgraded version to all brokers in the cluster after you determine that a version is safe after canary.")),(0,n.kt)("ol",{start:4},(0,n.kt)("li",{parentName:"ol"},"Upgrade proxies")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Canary test: test an upgraded version in one or a small set of proxies."),(0,n.kt)("li",{parentName:"ul"},"Rolling upgrade: rollout the upgraded version to all proxies in the cluster after you determine that a version is safe after canary.")),(0,n.kt)("h2",{id:"upgrade-zookeeper-optional"},"Upgrade ZooKeeper (optional)"),(0,n.kt)("p",null,"While you upgrade ZooKeeper servers, you can do canary test first, and then upgrade all ZooKeeper servers in the cluster."),(0,n.kt)("h3",{id:"canary-test"},"Canary test"),(0,n.kt)("p",null,"You can test an upgraded version in one of ZooKeeper servers before upgrading all ZooKeeper servers in your cluster."),(0,n.kt)("p",null,"To upgrade ZooKeeper server to a new version, complete the following steps:"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},"Stop a ZooKeeper server."),(0,n.kt)("li",{parentName:"ol"},"Upgrade the binary and configuration files."),(0,n.kt)("li",{parentName:"ol"},"Start the ZooKeeper server with the new binary files."),(0,n.kt)("li",{parentName:"ol"},"Use ",(0,n.kt)("inlineCode",{parentName:"li"},"pulsar zookeeper-shell")," to connect to the newly upgraded ZooKeeper server and run a few commands to verify if it works as expected."),(0,n.kt)("li",{parentName:"ol"},"Run the ZooKeeper server for a few days, observe and make sure the ZooKeeper cluster runs well.")),(0,n.kt)("h4",{id:"canary-rollback"},"Canary rollback"),(0,n.kt)("p",null,"If issues occur during canary test, you can shut down the problematic ZooKeeper node, revert the binary and configuration, and restart the ZooKeeper with the reverted binary."),(0,n.kt)("h3",{id:"upgrade-all-zookeeper-servers"},"Upgrade all ZooKeeper servers"),(0,n.kt)("p",null,"After canary test to upgrade one ZooKeeper in your cluster, you can upgrade all ZooKeeper servers in your cluster. "),(0,n.kt)("p",null,"You can upgrade all ZooKeeper servers one by one by following steps in canary test."),(0,n.kt)("h2",{id:"upgrade-bookies"},"Upgrade bookies"),(0,n.kt)("p",null,"While you upgrade bookies, you can do canary test first, and then upgrade all bookies in the cluster.\nFor more details, you can read Apache BookKeeper ",(0,n.kt)("a",{parentName:"p",href:"http://bookkeeper.apache.org/docs/latest/admin/upgrade"},"Upgrade guide"),"."),(0,n.kt)("h3",{id:"canary-test-1"},"Canary test"),(0,n.kt)("p",null,"You can test an upgraded version in one or a small set of bookies before upgrading all bookies in your cluster."),(0,n.kt)("p",null,"To upgrade bookie to a new version, complete the following steps:"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Stop a bookie.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Upgrade the binary and configuration files.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Start the bookie in ",(0,n.kt)("inlineCode",{parentName:"p"},"ReadOnly")," mode to verify if the bookie of this new version runs well for read workload."),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"\nbin/pulsar bookie --readOnly\n\n"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"When the bookie runs successfully in ",(0,n.kt)("inlineCode",{parentName:"p"},"ReadOnly")," mode, stop the bookie and restart it in ",(0,n.kt)("inlineCode",{parentName:"p"},"Write/Read")," mode."),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"\nbin/pulsar bookie\n\n"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Observe and make sure the cluster serves both write and read traffic."))),(0,n.kt)("h4",{id:"canary-rollback-1"},"Canary rollback"),(0,n.kt)("p",null,"If issues occur during the canary test, you can shut down the problematic bookie node. Other bookies in the cluster replaces this problematic bookie node with autorecovery. "),(0,n.kt)("h3",{id:"upgrade-all-bookies"},"Upgrade all bookies"),(0,n.kt)("p",null,"After canary test to upgrade some bookies in your cluster, you can upgrade all bookies in your cluster. "),(0,n.kt)("p",null,"Before upgrading, you have to decide whether to upgrade the whole cluster at once, including downtime and rolling upgrade scenarios."),(0,n.kt)("p",null,"In a rolling upgrade scenario, upgrade one bookie at a time. In a downtime upgrade scenario, shut down the entire cluster, upgrade each bookie, and then start the cluster."),(0,n.kt)("p",null,"While you upgrade in both scenarios, the procedure is the same for each bookie."),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},"Stop the bookie. "),(0,n.kt)("li",{parentName:"ol"},"Upgrade the software (either new binary or new configuration files)."),(0,n.kt)("li",{parentName:"ol"},"Start the bookie.")),(0,n.kt)("blockquote",null,(0,n.kt)("p",{parentName:"blockquote"},(0,n.kt)("strong",{parentName:"p"},"Advanced operations"),(0,n.kt)("br",{parentName:"p"}),"\n","When you upgrade a large BookKeeper cluster in a rolling upgrade scenario, upgrading one bookie at a time is slow. If you configure rack-aware or region-aware placement policy, you can upgrade bookies rack by rack or region by region, which speeds up the whole upgrade process.")),(0,n.kt)("h2",{id:"upgrade-brokers-and-proxies"},"Upgrade brokers and proxies"),(0,n.kt)("p",null,"The upgrade procedure for brokers and proxies is the same. Brokers and proxies are ",(0,n.kt)("inlineCode",{parentName:"p"},"stateless"),", so upgrading the two services is easy."),(0,n.kt)("h3",{id:"canary-test-2"},"Canary test"),(0,n.kt)("p",null,"You can test an upgraded version in one or a small set of nodes before upgrading all nodes in your cluster."),(0,n.kt)("p",null,"To upgrade to a new version, complete the following steps:"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},"Stop a broker (or proxy)."),(0,n.kt)("li",{parentName:"ol"},"Upgrade the binary and configuration file."),(0,n.kt)("li",{parentName:"ol"},"Start a broker (or proxy).")),(0,n.kt)("h4",{id:"canary-rollback-2"},"Canary rollback"),(0,n.kt)("p",null,"If issues occur during canary test, you can shut down the problematic broker (or proxy) node. Revert to the old version and restart the broker (or proxy)."),(0,n.kt)("h3",{id:"upgrade-all-brokers-or-proxies"},"Upgrade all brokers or proxies"),(0,n.kt)("p",null,"After canary test to upgrade some brokers or proxies in your cluster, you can upgrade all brokers or proxies in your cluster. "),(0,n.kt)("p",null,"Before upgrading, you have to decide whether to upgrade the whole cluster at once, including downtime and rolling upgrade scenarios."),(0,n.kt)("p",null,"In a rolling upgrade scenario, you can upgrade one broker or one proxy at a time if the size of the cluster is small. If your cluster is large, you can upgrade brokers or proxies in batches. When you upgrade a batch of brokers or proxies, make sure the remaining brokers and proxies in the cluster have enough capacity to handle the traffic during upgrade."),(0,n.kt)("p",null,"In a downtime upgrade scenario, shut down the entire cluster, upgrade each broker or proxy, and then start the cluster."),(0,n.kt)("p",null,"While you upgrade in both scenarios, the procedure is the same for each broker or proxy."),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},"Stop the broker or proxy. "),(0,n.kt)("li",{parentName:"ol"},"Upgrade the software (either new binary or new configuration files)."),(0,n.kt)("li",{parentName:"ol"},"Start the broker or proxy.")))}c.isMDXComponent=!0},86010:function(e,r,t){function a(e){var r,t,o="";if("string"==typeof e||"number"==typeof e)o+=e;else if("object"==typeof e)if(Array.isArray(e))for(r=0;r<e.length;r++)e[r]&&(t=a(e[r]))&&(o&&(o+=" "),o+=t);else for(r in e)e[r]&&(o&&(o+=" "),o+=r);return o}function o(){for(var e,r,t=0,o="";t<arguments.length;)(e=arguments[t++])&&(r=a(e))&&(o&&(o+=" "),o+=r);return o}t.d(r,{Z:function(){return o}})}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[88195],{3905:function(e,a,r){r.d(a,{Zo:function(){return p},kt:function(){return d}});var n=r(67294);function t(e,a,r){return a in e?Object.defineProperty(e,a,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[a]=r,e}function i(e,a){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);a&&(n=n.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var a=1;a<arguments.length;a++){var r=null!=arguments[a]?arguments[a]:{};a%2?i(Object(r),!0).forEach((function(a){t(e,a,r[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(r,a))}))}return e}function l(e,a){if(null==e)return{};var r,n,t=function(e,a){if(null==e)return{};var r,n,t={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],a.indexOf(r)>=0||(t[r]=e[r]);return t}(e,a);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],a.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(t[r]=e[r])}return t}var s=n.createContext({}),u=function(e){var a=n.useContext(s),r=a;return e&&(r="function"==typeof e?e(a):o(o({},a),e)),r},p=function(e){var a=u(e.components);return n.createElement(s.Provider,{value:a},e.children)},c={inlineCode:"code",wrapper:function(e){var a=e.children;return n.createElement(n.Fragment,{},a)}},m=n.forwardRef((function(e,a){var r=e.components,t=e.mdxType,i=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),m=u(r),d=t,f=m["".concat(s,".").concat(d)]||m[d]||c[d]||i;return r?n.createElement(f,o(o({ref:a},p),{},{components:r})):n.createElement(f,o({ref:a},p))}));function d(e,a){var r=arguments,t=a&&a.mdxType;if("string"==typeof e||t){var i=r.length,o=new Array(i);o[0]=m;var l={};for(var s in a)hasOwnProperty.call(a,s)&&(l[s]=a[s]);l.originalType=e,l.mdxType="string"==typeof e?e:t,o[1]=l;for(var u=2;u<i;u++)o[u]=r[u];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},58215:function(e,a,r){var n=r(67294);a.Z=function(e){var a=e.children,r=e.hidden,t=e.className;return n.createElement("div",{role:"tabpanel",hidden:r,className:t},a)}},55064:function(e,a,r){r.d(a,{Z:function(){return u}});var n=r(67294),t=r(79443);var i=function(){var e=(0,n.useContext)(t.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},o=r(86010),l="tabItem_1uMI",s="tabItemActive_2DSg";var u=function(e){var a,r=e.lazy,t=e.block,u=e.defaultValue,p=e.values,c=e.groupId,m=e.className,d=n.Children.toArray(e.children),f=null!=p?p:d.map((function(e){return{value:e.props.value,label:e.props.label}})),k=null!=u?u:null==(a=d.find((function(e){return e.props.default})))?void 0:a.props.value,b=i(),v=b.tabGroupChoices,g=b.setTabGroupChoices,h=(0,n.useState)(k),y=h[0],N=h[1],T=[];if(null!=c){var A=v[c];null!=A&&A!==y&&f.some((function(e){return e.value===A}))&&N(A)}var w=function(e){var a=e.currentTarget,r=T.indexOf(a),n=f[r].value;N(n),null!=c&&(g(c,n),setTimeout((function(){var e,r,n,t,i,o,l,u;(e=a.getBoundingClientRect(),r=e.top,n=e.left,t=e.bottom,i=e.right,o=window,l=o.innerHeight,u=o.innerWidth,r>=0&&i<=u&&t<=l&&n>=0)||(a.scrollIntoView({block:"center",behavior:"smooth"}),a.classList.add(s),setTimeout((function(){return a.classList.remove(s)}),2e3))}),150))},x=function(e){var a,r=null;switch(e.key){case"ArrowRight":var n=T.indexOf(e.target)+1;r=T[n]||T[0];break;case"ArrowLeft":var t=T.indexOf(e.target)-1;r=T[t]||T[T.length-1]}null==(a=r)||a.focus()};return n.createElement("div",{className:"tabs-container"},n.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.Z)("tabs",{"tabs--block":t},m)},f.map((function(e){var a=e.value,r=e.label;return n.createElement("li",{role:"tab",tabIndex:y===a?0:-1,"aria-selected":y===a,className:(0,o.Z)("tabs__item",l,{"tabs__item--active":y===a}),key:a,ref:function(e){return T.push(e)},onKeyDown:x,onFocus:w,onClick:w},null!=r?r:a)}))),r?(0,n.cloneElement)(d.filter((function(e){return e.props.value===y}))[0],{className:"margin-vert--md"}):n.createElement("div",{className:"margin-vert--md"},d.map((function(e,a){return(0,n.cloneElement)(e,{key:a,hidden:e.props.value!==y})}))))}},79443:function(e,a,r){var n=(0,r(67294).createContext)(void 0);a.Z=n},82282:function(e,a,r){r.r(a),r.d(a,{frontMatter:function(){return u},contentTitle:function(){return p},metadata:function(){return c},toc:function(){return m},default:function(){return f}});var n=r(87462),t=r(63366),i=(r(67294),r(3905)),o=r(55064),l=r(58215),s=["components"],u={id:"admin-api-brokers",title:"Managing Brokers",sidebar_label:"Brokers",original_id:"admin-api-brokers"},p=void 0,c={unversionedId:"admin-api-brokers",id:"version-2.7.1/admin-api-brokers",isDocsHomePage:!1,title:"Managing Brokers",description:"Pulsar brokers consist of two components:",source:"@site/versioned_docs/version-2.7.1/admin-api-brokers.md",sourceDirName:".",slug:"/admin-api-brokers",permalink:"/docs/2.7.1/admin-api-brokers",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.1/admin-api-brokers.md",tags:[],version:"2.7.1",frontMatter:{id:"admin-api-brokers",title:"Managing Brokers",sidebar_label:"Brokers",original_id:"admin-api-brokers"},sidebar:"version-2.7.1/docsSidebar",previous:{title:"Tenants",permalink:"/docs/2.7.1/admin-api-tenants"},next:{title:"Namespaces",permalink:"/docs/2.7.1/admin-api-namespaces"}},m=[{value:"Brokers resources",id:"brokers-resources",children:[{value:"List active brokers",id:"list-active-brokers",children:[]},{value:"Dynamic broker configuration",id:"dynamic-broker-configuration",children:[]},{value:"Update dynamic configuration",id:"update-dynamic-configuration",children:[]},{value:"List updated values",id:"list-updated-values",children:[]},{value:"List all",id:"list-all",children:[]}]}],d={toc:m};function f(e){var a=e.components,r=(0,t.Z)(e,s);return(0,i.kt)("wrapper",(0,n.Z)({},d,r,{components:a,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Pulsar brokers consist of two components:"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},"An HTTP server exposing a ",(0,i.kt)("a",{parentName:"li",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"REST")," interface administration and ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.7.1/reference-terminology#topic"},"topic")," lookup."),(0,i.kt)("li",{parentName:"ol"},"A dispatcher that handles all Pulsar ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.7.1/reference-terminology#message"},"message")," transfers.")),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.1/reference-terminology#broker"},"Brokers")," can be managed via:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.7.1/pulsar-admin#brokers"},(0,i.kt)("inlineCode",{parentName:"a"},"brokers"))," command of the ",(0,i.kt)("a",{parentName:"li",href:"reference-pulsar-admin"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar-admin"))," tool"),(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("inlineCode",{parentName:"li"},"/admin/v2/brokers")," endpoint of the admin ",(0,i.kt)("a",{parentName:"li",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"REST")," API"),(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("inlineCode",{parentName:"li"},"brokers")," method of the ",(0,i.kt)("a",{parentName:"li",href:"https://pulsar.incubator.apache.org/api/admin/org/apache/pulsar/client/admin/PulsarAdmin.html"},"PulsarAdmin")," object in the ",(0,i.kt)("a",{parentName:"li",href:"client-libraries-java"},"Java API"))),(0,i.kt)("p",null,"In addition to being configurable when you start them up, brokers can also be ",(0,i.kt)("a",{parentName:"p",href:"#dynamic-broker-configuration"},"dynamically configured"),"."),(0,i.kt)("blockquote",null,(0,i.kt)("p",{parentName:"blockquote"},"See the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.1/reference-configuration#broker"},"Configuration")," page for a full listing of broker-specific configuration parameters.")),(0,i.kt)("h2",{id:"brokers-resources"},"Brokers resources"),(0,i.kt)("h3",{id:"list-active-brokers"},"List active brokers"),(0,i.kt)("p",null,"Fetch all available active brokers that are serving traffic."),(0,i.kt)(o.Z,{defaultValue:"pulsar-admin",values:[{label:"pulsar-admin",value:"pulsar-admin"},{label:"REST API",value:"REST API"},{label:"JAVA",value:"JAVA"}],mdxType:"Tabs"},(0,i.kt)(l.Z,{value:"pulsar-admin",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin brokers list use\n\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\nbroker1.use.org.com:8080\n\n"))),(0,i.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getActiveBrokers?version=2.7.1&apiVersion=v2"},"GET /admin/v2/brokers/:cluster"))),(0,i.kt)(l.Z,{value:"JAVA",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.brokers().getActiveBrokers(clusterName)\n\n")))),(0,i.kt)("h4",{id:"list-of-namespaces-owned-by-a-given-broker"},"list of namespaces owned by a given broker"),(0,i.kt)("p",null,"It finds all namespaces which are owned and served by a given broker."),(0,i.kt)(o.Z,{defaultValue:"pulsar-admin",values:[{label:"pulsar-admin",value:"pulsar-admin"},{label:"REST API",value:"REST API"},{label:"JAVA",value:"JAVA"}],mdxType:"Tabs"},(0,i.kt)(l.Z,{value:"pulsar-admin",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin brokers namespaces use \\\n  --url broker1.use.org.com:8080\n\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "my-property/use/my-ns/0x00000000_0xffffffff": {\n    "broker_assignment": "shared",\n    "is_controlled": false,\n    "is_active": true\n  }\n}\n\n'))),(0,i.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getOwnedNamespaes?version=2.7.1&apiVersion=v2"},"GET /admin/v2/brokers/:cluster/:broker/ownedNamespaces"))),(0,i.kt)(l.Z,{value:"JAVA",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.brokers().getOwnedNamespaces(cluster,brokerUrl);\n\n")))),(0,i.kt)("h3",{id:"dynamic-broker-configuration"},"Dynamic broker configuration"),(0,i.kt)("p",null,"One way to configure a Pulsar ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.1/reference-terminology#broker"},"broker")," is to supply a ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.1/reference-configuration#broker"},"configuration")," when the broker is ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.1/reference-cli-tools#pulsar-broker"},"started up"),"."),(0,i.kt)("p",null,"But since all broker configuration in Pulsar is stored in ZooKeeper, configuration values can also be dynamically updated ",(0,i.kt)("em",{parentName:"p"},"while the broker is running"),". When you update broker configuration dynamically, ZooKeeper will notify the broker of the change and the broker will then override any existing configuration values."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.7.1/pulsar-admin#brokers"},(0,i.kt)("inlineCode",{parentName:"a"},"brokers"))," command for the ",(0,i.kt)("a",{parentName:"li",href:"reference-pulsar-admin"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar-admin"))," tool has a variety of subcommands that enable you to manipulate a broker's configuration dynamically, enabling you to ",(0,i.kt)("a",{parentName:"li",href:"#update-dynamic-configuration"},"update config values")," and more."),(0,i.kt)("li",{parentName:"ul"},"In the Pulsar admin ",(0,i.kt)("a",{parentName:"li",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"REST")," API, dynamic configuration is managed through the ",(0,i.kt)("inlineCode",{parentName:"li"},"/admin/v2/brokers/configuration")," endpoint.")),(0,i.kt)("h3",{id:"update-dynamic-configuration"},"Update dynamic configuration"),(0,i.kt)(o.Z,{defaultValue:"pulsar-admin",values:[{label:"pulsar-admin",value:"pulsar-admin"},{label:"REST API",value:"REST API"},{label:"JAVA",value:"JAVA"}],mdxType:"Tabs"},(0,i.kt)(l.Z,{value:"pulsar-admin",mdxType:"TabItem"},(0,i.kt)("p",null,"The ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#brokers-update-dynamic-config"},(0,i.kt)("inlineCode",{parentName:"a"},"update-dynamic-config"))," subcommand will update existing configuration. It takes two arguments: the name of the parameter and the new value using the ",(0,i.kt)("inlineCode",{parentName:"p"},"config")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"value")," flag respectively. Here's an example for the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.1/reference-configuration#broker-brokerShutdownTimeoutMs"},(0,i.kt)("inlineCode",{parentName:"a"},"brokerShutdownTimeoutMs"))," parameter:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin brokers update-dynamic-config --config brokerShutdownTimeoutMs --value 100\n\n"))),(0,i.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/updateDynamicConfiguration?version=2.7.1&apiVersion=v2"},"POST /admin/v2/brokers/configuration/:configName/:configValue"))),(0,i.kt)(l.Z,{value:"JAVA",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.brokers().updateDynamicConfiguration(configName, configValue);\n\n")))),(0,i.kt)("h3",{id:"list-updated-values"},"List updated values"),(0,i.kt)("p",null,"Fetch a list of all potentially updatable configuration parameters."),(0,i.kt)(o.Z,{defaultValue:"pulsar-admin",values:[{label:"pulsar-admin",value:"pulsar-admin"},{label:"REST API",value:"REST API"},{label:"JAVA",value:"JAVA"}],mdxType:"Tabs"},(0,i.kt)(l.Z,{value:"pulsar-admin",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin brokers list-dynamic-config\nbrokerShutdownTimeoutMs\n\n"))),(0,i.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getDynamicConfigurationName?version=2.7.1&apiVersion=v2"},"GET /admin/v2/brokers/configuration"))),(0,i.kt)(l.Z,{value:"JAVA",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.brokers().getDynamicConfigurationNames();\n\n")))),(0,i.kt)("h3",{id:"list-all"},"List all"),(0,i.kt)("p",null,"Fetch a list of all parameters that have been dynamically updated."),(0,i.kt)(o.Z,{defaultValue:"pulsar-admin",values:[{label:"pulsar-admin",value:"pulsar-admin"},{label:"REST API",value:"REST API"},{label:"JAVA",value:"JAVA"}],mdxType:"Tabs"},(0,i.kt)(l.Z,{value:"pulsar-admin",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin brokers get-all-dynamic-config\nbrokerShutdownTimeoutMs:100\n\n"))),(0,i.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#operation/getAllDynamicConfigurations?version=2.7.1&apiVersion=v2"},"GET /admin/v2/brokers/configuration/values"))),(0,i.kt)(l.Z,{value:"JAVA",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.brokers().getAllDynamicConfigurations();\n\n")))))}f.isMDXComponent=!0},86010:function(e,a,r){function n(e){var a,r,t="";if("string"==typeof e||"number"==typeof e)t+=e;else if("object"==typeof e)if(Array.isArray(e))for(a=0;a<e.length;a++)e[a]&&(r=n(e[a]))&&(t&&(t+=" "),t+=r);else for(a in e)e[a]&&(t&&(t+=" "),t+=a);return t}function t(){for(var e,a,r=0,t="";r<arguments.length;)(e=arguments[r++])&&(a=n(e))&&(t&&(t+=" "),t+=a);return t}r.d(a,{Z:function(){return t}})}}]);
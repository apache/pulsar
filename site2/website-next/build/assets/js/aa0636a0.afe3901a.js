"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[19913],{3905:function(n,a,e){e.d(a,{Zo:function(){return c},kt:function(){return d}});var t=e(67294);function i(n,a,e){return a in n?Object.defineProperty(n,a,{value:e,enumerable:!0,configurable:!0,writable:!0}):n[a]=e,n}function s(n,a){var e=Object.keys(n);if(Object.getOwnPropertySymbols){var t=Object.getOwnPropertySymbols(n);a&&(t=t.filter((function(a){return Object.getOwnPropertyDescriptor(n,a).enumerable}))),e.push.apply(e,t)}return e}function u(n){for(var a=1;a<arguments.length;a++){var e=null!=arguments[a]?arguments[a]:{};a%2?s(Object(e),!0).forEach((function(a){i(n,a,e[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(n,Object.getOwnPropertyDescriptors(e)):s(Object(e)).forEach((function(a){Object.defineProperty(n,a,Object.getOwnPropertyDescriptor(e,a))}))}return n}function l(n,a){if(null==n)return{};var e,t,i=function(n,a){if(null==n)return{};var e,t,i={},s=Object.keys(n);for(t=0;t<s.length;t++)e=s[t],a.indexOf(e)>=0||(i[e]=n[e]);return i}(n,a);if(Object.getOwnPropertySymbols){var s=Object.getOwnPropertySymbols(n);for(t=0;t<s.length;t++)e=s[t],a.indexOf(e)>=0||Object.prototype.propertyIsEnumerable.call(n,e)&&(i[e]=n[e])}return i}var o=t.createContext({}),r=function(n){var a=t.useContext(o),e=a;return n&&(e="function"==typeof n?n(a):u(u({},a),n)),e},c=function(n){var a=r(n.components);return t.createElement(o.Provider,{value:a},n.children)},m={inlineCode:"code",wrapper:function(n){var a=n.children;return t.createElement(t.Fragment,{},a)}},p=t.forwardRef((function(n,a){var e=n.components,i=n.mdxType,s=n.originalType,o=n.parentName,c=l(n,["components","mdxType","originalType","parentName"]),p=r(e),d=i,f=p["".concat(o,".").concat(d)]||p[d]||m[d]||s;return e?t.createElement(f,u(u({ref:a},c),{},{components:e})):t.createElement(f,u({ref:a},c))}));function d(n,a){var e=arguments,i=a&&a.mdxType;if("string"==typeof n||i){var s=e.length,u=new Array(s);u[0]=p;var l={};for(var o in a)hasOwnProperty.call(a,o)&&(l[o]=a[o]);l.originalType=n,l.mdxType="string"==typeof n?n:i,u[1]=l;for(var r=2;r<s;r++)u[r]=e[r];return t.createElement.apply(null,u)}return t.createElement.apply(null,e)}p.displayName="MDXCreateElement"},58215:function(n,a,e){var t=e(67294);a.Z=function(n){var a=n.children,e=n.hidden,i=n.className;return t.createElement("div",{role:"tabpanel",hidden:e,className:i},a)}},55064:function(n,a,e){e.d(a,{Z:function(){return r}});var t=e(67294),i=e(79443);var s=function(){var n=(0,t.useContext)(i.Z);if(null==n)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return n},u=e(86010),l="tabItem_1uMI",o="tabItemActive_2DSg";var r=function(n){var a,e=n.lazy,i=n.block,r=n.defaultValue,c=n.values,m=n.groupId,p=n.className,d=t.Children.toArray(n.children),f=null!=c?c:d.map((function(n){return{value:n.props.value,label:n.props.label}})),v=null!=r?r:null==(a=d.find((function(n){return n.props.default})))?void 0:a.props.value,k=s(),I=k.tabGroupChoices,A=k.setTabGroupChoices,g=(0,t.useState)(v),h=g[0],b=g[1],T=[];if(null!=m){var N=I[m];null!=N&&N!==h&&f.some((function(n){return n.value===N}))&&b(N)}var P=function(n){var a=n.currentTarget,e=T.indexOf(a),t=f[e].value;b(t),null!=m&&(A(m,t),setTimeout((function(){var n,e,t,i,s,u,l,r;(n=a.getBoundingClientRect(),e=n.top,t=n.left,i=n.bottom,s=n.right,u=window,l=u.innerHeight,r=u.innerWidth,e>=0&&s<=r&&i<=l&&t>=0)||(a.scrollIntoView({block:"center",behavior:"smooth"}),a.classList.add(o),setTimeout((function(){return a.classList.remove(o)}),2e3))}),150))},C=function(n){var a,e=null;switch(n.key){case"ArrowRight":var t=T.indexOf(n.target)+1;e=T[t]||T[0];break;case"ArrowLeft":var i=T.indexOf(n.target)-1;e=T[i]||T[T.length-1]}null==(a=e)||a.focus()};return t.createElement("div",{className:"tabs-container"},t.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,u.Z)("tabs",{"tabs--block":i},p)},f.map((function(n){var a=n.value,e=n.label;return t.createElement("li",{role:"tab",tabIndex:h===a?0:-1,"aria-selected":h===a,className:(0,u.Z)("tabs__item",l,{"tabs__item--active":h===a}),key:a,ref:function(n){return T.push(n)},onKeyDown:C,onFocus:P,onClick:P},null!=e?e:a)}))),e?(0,t.cloneElement)(d.filter((function(n){return n.props.value===h}))[0],{className:"margin-vert--md"}):t.createElement("div",{className:"margin-vert--md"},d.map((function(n,a){return(0,t.cloneElement)(n,{key:a,hidden:n.props.value!==h})}))))}},79443:function(n,a,e){var t=(0,e(67294).createContext)(void 0);a.Z=t},96040:function(n,a,e){e.r(a),e.d(a,{frontMatter:function(){return r},contentTitle:function(){return c},metadata:function(){return m},toc:function(){return p},default:function(){return f}});var t=e(87462),i=e(63366),s=(e(67294),e(3905)),u=e(55064),l=e(58215),o=["components"],r={id:"admin-api-functions",title:"Manage Functions",sidebar_label:"Functions",original_id:"admin-api-functions"},c=void 0,m={unversionedId:"admin-api-functions",id:"version-2.7.1/admin-api-functions",isDocsHomePage:!1,title:"Manage Functions",description:"Pulsar Functions are lightweight compute processes that",source:"@site/versioned_docs/version-2.7.1/admin-api-functions.md",sourceDirName:".",slug:"/admin-api-functions",permalink:"/docs/2.7.1/admin-api-functions",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.1/admin-api-functions.md",tags:[],version:"2.7.1",frontMatter:{id:"admin-api-functions",title:"Manage Functions",sidebar_label:"Functions",original_id:"admin-api-functions"},sidebar:"version-2.7.1/docsSidebar",previous:{title:"Topics",permalink:"/docs/2.7.1/admin-api-topics"},next:{title:"Kafka client wrapper",permalink:"/docs/2.7.1/adaptors-kafka"}},p=[{value:"Function resources",id:"function-resources",children:[{value:"Create a function",id:"create-a-function",children:[]},{value:"Update a function",id:"update-a-function",children:[]},{value:"Start an instance of a function",id:"start-an-instance-of-a-function",children:[]},{value:"Start all instances of a function",id:"start-all-instances-of-a-function",children:[]},{value:"Stop an instance of a function",id:"stop-an-instance-of-a-function",children:[]},{value:"Stop all instances of a function",id:"stop-all-instances-of-a-function",children:[]},{value:"Restart an instance of a function",id:"restart-an-instance-of-a-function",children:[]},{value:"Restart all instances of a function",id:"restart-all-instances-of-a-function",children:[]},{value:"List all functions",id:"list-all-functions",children:[]},{value:"Delete a function",id:"delete-a-function",children:[]},{value:"Get info about a function",id:"get-info-about-a-function",children:[]},{value:"Get status of an instance of a function",id:"get-status-of-an-instance-of-a-function",children:[]},{value:"Get status of all instances of a function",id:"get-status-of-all-instances-of-a-function",children:[]},{value:"Get stats of an instance of a function",id:"get-stats-of-an-instance-of-a-function",children:[]},{value:"Get stats of all instances of a function",id:"get-stats-of-all-instances-of-a-function",children:[]},{value:"Trigger a function",id:"trigger-a-function",children:[]},{value:"Put state associated with a function",id:"put-state-associated-with-a-function",children:[]},{value:"Fetch state associated with a function",id:"fetch-state-associated-with-a-function",children:[]}]}],d={toc:p};function f(n){var a=n.components,e=(0,i.Z)(n,o);return(0,s.kt)("wrapper",(0,t.Z)({},d,e,{components:a,mdxType:"MDXLayout"}),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Pulsar Functions")," are lightweight compute processes that"),(0,s.kt)("ul",null,(0,s.kt)("li",{parentName:"ul"},"consume messages from one or more Pulsar topics"),(0,s.kt)("li",{parentName:"ul"},"apply a user-supplied processing logic to each message"),(0,s.kt)("li",{parentName:"ul"},"publish the results of the computation to another topic")),(0,s.kt)("p",null,"Functions can be managed via the following methods."),(0,s.kt)("table",null,(0,s.kt)("thead",{parentName:"table"},(0,s.kt)("tr",{parentName:"thead"},(0,s.kt)("th",{parentName:"tr",align:null},"Method"),(0,s.kt)("th",{parentName:"tr",align:null},"Description"))),(0,s.kt)("tbody",{parentName:"table"},(0,s.kt)("tr",{parentName:"tbody"},(0,s.kt)("td",{parentName:"tr",align:null},(0,s.kt)("strong",{parentName:"td"},"Admin CLI")),(0,s.kt)("td",{parentName:"tr",align:null},"The ",(0,s.kt)("a",{parentName:"td",href:"/docs/2.7.1/pulsar-admin#functions"},(0,s.kt)("inlineCode",{parentName:"a"},"functions"))," command of the ",(0,s.kt)("a",{parentName:"td",href:"reference-pulsar-admin"},(0,s.kt)("inlineCode",{parentName:"a"},"pulsar-admin"))," tool.")),(0,s.kt)("tr",{parentName:"tbody"},(0,s.kt)("td",{parentName:"tr",align:null},(0,s.kt)("strong",{parentName:"td"},"REST API")),(0,s.kt)("td",{parentName:"tr",align:null},"The ",(0,s.kt)("inlineCode",{parentName:"td"},"/admin/v3/functions")," endpoint of the admin ",(0,s.kt)("a",{parentName:"td",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"REST")," API.")),(0,s.kt)("tr",{parentName:"tbody"},(0,s.kt)("td",{parentName:"tr",align:null},(0,s.kt)("strong",{parentName:"td"},"Java Admin API")),(0,s.kt)("td",{parentName:"tr",align:null},"The ",(0,s.kt)("inlineCode",{parentName:"td"},"functions")," method of the ",(0,s.kt)("a",{parentName:"td",href:"https://pulsar.incubator.apache.org/api/admin/org/apache/pulsar/client/admin/PulsarAdmin"},"PulsarAdmin")," object in the ",(0,s.kt)("a",{parentName:"td",href:"client-libraries-java"},"Java API"),".")))),(0,s.kt)("h2",{id:"function-resources"},"Function resources"),(0,s.kt)("p",null,"You can perform the following operations on functions."),(0,s.kt)("h3",{id:"create-a-function"},"Create a function"),(0,s.kt)("p",null,"You can create a Pulsar function in cluster mode (deploy it on a Pulsar cluster) using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-create"},(0,s.kt)("inlineCode",{parentName:"a"},"create"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions create \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --inputs test-input-topic \\\n  --output persistent://public/default/test-output-topic \\\n  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \\\n  --jar /examples/api-examples.jar\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},'\nFunctionConfig functionConfig = new FunctionConfig();\nfunctionConfig.setTenant(tenant);\nfunctionConfig.setNamespace(namespace);\nfunctionConfig.setName(functionName);\nfunctionConfig.setRuntime(FunctionConfig.Runtime.JAVA);\nfunctionConfig.setParallelism(1);\nfunctionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");\nfunctionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);\nfunctionConfig.setTopicsPattern(sourceTopicPattern);\nfunctionConfig.setSubName(subscriptionName);\nfunctionConfig.setAutoAck(true);\nfunctionConfig.setOutput(sinkTopic);\nadmin.functions().createFunction(functionConfig, fileName);\n\n')))),(0,s.kt)("h3",{id:"update-a-function"},"Update a function"),(0,s.kt)("p",null,"You can update a Pulsar function that has been deployed to a Pulsar cluster using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST Admin API",value:"REST Admin API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-update"},(0,s.kt)("inlineCode",{parentName:"a"},"update"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions update \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --output persistent://public/default/update-output-topic \\\n  # other options\n\n"))),(0,s.kt)(l.Z,{value:"REST Admin API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1&apiVersion=v3"},"PUT /admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},'\nFunctionConfig functionConfig = new FunctionConfig();\nfunctionConfig.setTenant(tenant);\nfunctionConfig.setNamespace(namespace);\nfunctionConfig.setName(functionName);\nfunctionConfig.setRuntime(FunctionConfig.Runtime.JAVA);\nfunctionConfig.setParallelism(1);\nfunctionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");\nUpdateOptions updateOptions = new UpdateOptions();\nupdateOptions.setUpdateAuthData(updateAuthData);\nadmin.functions().updateFunction(functionConfig, userCodeFile, updateOptions);\n\n')))),(0,s.kt)("h3",{id:"start-an-instance-of-a-function"},"Start an instance of a function"),(0,s.kt)("p",null,"You can start a stopped function instance with ",(0,s.kt)("inlineCode",{parentName:"p"},"instance-id")," using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-start"},(0,s.kt)("inlineCode",{parentName:"a"},"start"))," subcommand. "),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions start \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --instance-id 1\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/start?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/start?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().startFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));\n\n")))),(0,s.kt)("h3",{id:"start-all-instances-of-a-function"},"Start all instances of a function"),(0,s.kt)("p",null,"You can start all stopped function instances using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java",value:"Java"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-start"},(0,s.kt)("inlineCode",{parentName:"a"},"start"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions start \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/start?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/start?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().startFunction(tenant, namespace, functionName);\n\n")))),(0,s.kt)("h3",{id:"stop-an-instance-of-a-function"},"Stop an instance of a function"),(0,s.kt)("p",null,"You can stop a function instance with ",(0,s.kt)("inlineCode",{parentName:"p"},"instance-id")," using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-stop"},(0,s.kt)("inlineCode",{parentName:"a"},"stop"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions stop \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --instance-id 1\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/stop?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/stop?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().stopFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));\n\n")))),(0,s.kt)("h3",{id:"stop-all-instances-of-a-function"},"Stop all instances of a function"),(0,s.kt)("p",null,"You can stop all function instances using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-stop"},(0,s.kt)("inlineCode",{parentName:"a"},"stop"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions stop \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/stop?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/stop?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().stopFunction(tenant, namespace, functionName);\n\n")))),(0,s.kt)("h3",{id:"restart-an-instance-of-a-function"},"Restart an instance of a function"),(0,s.kt)("p",null,"Restart a function instance with ",(0,s.kt)("inlineCode",{parentName:"p"},"instance-id")," using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-restart"},(0,s.kt)("inlineCode",{parentName:"a"},"restart"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions restart \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --instance-id 1\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/restart?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/restart?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().restartFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));\n\n")))),(0,s.kt)("h3",{id:"restart-all-instances-of-a-function"},"Restart all instances of a function"),(0,s.kt)("p",null,"You can restart all function instances using Admin CLI, REST API or Java admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-restart"},(0,s.kt)("inlineCode",{parentName:"a"},"restart"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions restart \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/restart?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/restart?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().restartFunction(tenant, namespace, functionName);\n\n")))),(0,s.kt)("h3",{id:"list-all-functions"},"List all functions"),(0,s.kt)("p",null,"You can list all Pulsar functions running under a specific tenant and namespace using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-list"},(0,s.kt)("inlineCode",{parentName:"a"},"list"))," subcommand."),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions list \\\n  --tenant public \\\n  --namespace default\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace?version=2.7.1&apiVersion=v3"},"GET /admin/v3/functions/:tenant/:namespace?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().getFunctions(tenant, namespace);\n\n")))),(0,s.kt)("h3",{id:"delete-a-function"},"Delete a function"),(0,s.kt)("p",null,"You can delete a Pulsar function that is running on a Pulsar cluster using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-delete"},(0,s.kt)("inlineCode",{parentName:"a"},"delete"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions delete \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions)\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1&apiVersion=v3"},"DELETE /admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().deleteFunction(tenant, namespace, functionName);\n\n")))),(0,s.kt)("h3",{id:"get-info-about-a-function"},"Get info about a function"),(0,s.kt)("p",null,"You can get information about a Pulsar function currently running in cluster mode using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-get"},(0,s.kt)("inlineCode",{parentName:"a"},"get"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions get \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions)\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1&apiVersion=v3"},"GET /admin/v3/functions/:tenant/:namespace/:functionName?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().getFunction(tenant, namespace, functionName);\n\n")))),(0,s.kt)("h3",{id:"get-status-of-an-instance-of-a-function"},"Get status of an instance of a function"),(0,s.kt)("p",null,"You can get the current status of a Pulsar function instance with ",(0,s.kt)("inlineCode",{parentName:"p"},"instance-id")," using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-status"},(0,s.kt)("inlineCode",{parentName:"a"},"status"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions status \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --instance-id 1\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/status?version=2.7.1&apiVersion=v3"},"GET /admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/status?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().getFunctionStatus(tenant, namespace, functionName, Integer.parseInt(instanceId));\n\n")))),(0,s.kt)("h3",{id:"get-status-of-all-instances-of-a-function"},"Get status of all instances of a function"),(0,s.kt)("p",null,"You can get the current status of a Pulsar function instance using Admin CLI, REST API or Java Admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-status"},(0,s.kt)("inlineCode",{parentName:"a"},"status"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions status \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions)\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/status?version=2.7.1&apiVersion=v3"},"GET /admin/v3/functions/:tenant/:namespace/:functionName/status?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().getFunctionStatus(tenant, namespace, functionName);\n\n")))),(0,s.kt)("h3",{id:"get-stats-of-an-instance-of-a-function"},"Get stats of an instance of a function"),(0,s.kt)("p",null,"You can get the current stats of a Pulsar Function instance with ",(0,s.kt)("inlineCode",{parentName:"p"},"instance-id")," using Admin CLI, REST API or Java admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-stats"},(0,s.kt)("inlineCode",{parentName:"a"},"stats"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions stats \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --instance-id 1\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/stats?version=2.7.1&apiVersion=v3"},"GET /admin/v3/functions/:tenant/:namespace/:functionName/:instanceId/stats?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().getFunctionStats(tenant, namespace, functionName, Integer.parseInt(instanceId));\n\n")))),(0,s.kt)("h3",{id:"get-stats-of-all-instances-of-a-function"},"Get stats of all instances of a function"),(0,s.kt)("p",null,"You can get the current stats of a Pulsar function using Admin CLI, REST API or Java admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-stats"},(0,s.kt)("inlineCode",{parentName:"a"},"stats"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions stats \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions)\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/stats?version=2.7.1&apiVersion=v3"},"GET /admin/v3/functions/:tenant/:namespace/:functionName/stats?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().getFunctionStats(tenant, namespace, functionName);\n\n")))),(0,s.kt)("h3",{id:"trigger-a-function"},"Trigger a function"),(0,s.kt)("p",null,"You can trigger a specified Pulsar function with a supplied value using Admin CLI, REST API or Java admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-trigger"},(0,s.kt)("inlineCode",{parentName:"a"},"trigger"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},'\n$ pulsar-admin functions trigger \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --topic (the name of input topic) \\\n  --trigger-value \\"hello pulsar\\"\n  # or --trigger-file (the path of trigger file)\n\n'))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/trigger?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/trigger?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().triggerFunction(tenant, namespace, functionName, topic, triggerValue, triggerFile);\n\n")))),(0,s.kt)("h3",{id:"put-state-associated-with-a-function"},"Put state associated with a function"),(0,s.kt)("p",null,"You can put the state associated with a Pulsar function using Admin CLI, REST API or Java admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin API",value:"Java Admin API"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-putstate"},(0,s.kt)("inlineCode",{parentName:"a"},"putstate"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},'\n$ pulsar-admin functions putstate \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --state "{\\"key\\":\\"pulsar\\", \\"stringValue\\":\\"hello pulsar\\"}"\n\n'))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/state/:key?version=2.7.1&apiVersion=v3"},"POST /admin/v3/functions/:tenant/:namespace/:functionName/state/:key?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin API",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nTypeReference<FunctionState> typeRef = new TypeReference<FunctionState>() {};\nFunctionState stateRepr = ObjectMapperFactory.getThreadLocal().readValue(state, typeRef);\nadmin.functions().putFunctionState(tenant, namespace, functionName, stateRepr);\n\n")))),(0,s.kt)("h3",{id:"fetch-state-associated-with-a-function"},"Fetch state associated with a function"),(0,s.kt)("p",null,"You can fetch the current state associated with a Pulsar function using Admin CLI, REST API or Java admin API."),(0,s.kt)(u.Z,{defaultValue:"Admin CLI",values:[{label:"Admin CLI",value:"Admin CLI"},{label:"REST API",value:"REST API"},{label:"Java Admin CLI",value:"Java Admin CLI"}],mdxType:"Tabs"},(0,s.kt)(l.Z,{value:"Admin CLI",mdxType:"TabItem"},(0,s.kt)("p",null,"Use the ",(0,s.kt)("a",{parentName:"p",href:"/docs/2.7.1/pulsar-admin#functions-querystate"},(0,s.kt)("inlineCode",{parentName:"a"},"querystate"))," subcommand. "),(0,s.kt)("p",null,(0,s.kt)("strong",{parentName:"p"},"Example")),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin functions querystate \\\n  --tenant public \\\n  --namespace default \\\n  --name (the name of Pulsar Functions) \\\n  --key (the key of state)\n\n"))),(0,s.kt)(l.Z,{value:"REST API",mdxType:"TabItem"},(0,s.kt)("p",null,(0,s.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/functions-rest-api#/admin/v3/functions/:tenant/:namespace/:functionName/state/:key?version=2.7.1&apiVersion=v3"},"GET /admin/v3/functions/:tenant/:namespace/:functionName/state/:key?version=2.7.1"))),(0,s.kt)(l.Z,{value:"Java Admin CLI",mdxType:"TabItem"},(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-java"},"\nadmin.functions().getFunctionState(tenant, namespace, functionName, key);\n\n")))))}f.isMDXComponent=!0},86010:function(n,a,e){function t(n){var a,e,i="";if("string"==typeof n||"number"==typeof n)i+=n;else if("object"==typeof n)if(Array.isArray(n))for(a=0;a<n.length;a++)n[a]&&(e=t(n[a]))&&(i&&(i+=" "),i+=e);else for(a in n)n[a]&&(i&&(i+=" "),i+=a);return i}function i(){for(var n,a,e=0,i="";e<arguments.length;)(n=arguments[e++])&&(a=t(n))&&(i&&(i+=" "),i+=a);return i}e.d(a,{Z:function(){return i}})}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[64095],{3905:function(e,t,n){n.d(t,{Zo:function(){return u},kt:function(){return m}});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),d=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=d(e.components);return a.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},p=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),p=d(n),m=r,f=p["".concat(s,".").concat(m)]||p[m]||c[m]||i;return n?a.createElement(f,o(o({ref:t},u),{},{components:n})):a.createElement(f,o({ref:t},u))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=p;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:r,o[1]=l;for(var d=2;d<i;d++)o[d]=n[d];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}p.displayName="MDXCreateElement"},58215:function(e,t,n){var a=n(67294);t.Z=function(e){var t=e.children,n=e.hidden,r=e.className;return a.createElement("div",{role:"tabpanel",hidden:n,className:r},t)}},55064:function(e,t,n){n.d(t,{Z:function(){return d}});var a=n(67294),r=n(79443);var i=function(){var e=(0,a.useContext)(r.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},o=n(86010),l="tabItem_1uMI",s="tabItemActive_2DSg";var d=function(e){var t,n=e.lazy,r=e.block,d=e.defaultValue,u=e.values,c=e.groupId,p=e.className,m=a.Children.toArray(e.children),f=null!=u?u:m.map((function(e){return{value:e.props.value,label:e.props.label}})),k=null!=d?d:null==(t=m.find((function(e){return e.props.default})))?void 0:t.props.value,b=i(),h=b.tabGroupChoices,g=b.setTabGroupChoices,v=(0,a.useState)(k),N=v[0],y=v[1],C=[];if(null!=c){var w=h[c];null!=w&&w!==N&&f.some((function(e){return e.value===w}))&&y(w)}var P=function(e){var t=e.currentTarget,n=C.indexOf(t),a=f[n].value;y(a),null!=c&&(g(c,a),setTimeout((function(){var e,n,a,r,i,o,l,d;(e=t.getBoundingClientRect(),n=e.top,a=e.left,r=e.bottom,i=e.right,o=window,l=o.innerHeight,d=o.innerWidth,n>=0&&i<=d&&r<=l&&a>=0)||(t.scrollIntoView({block:"center",behavior:"smooth"}),t.classList.add(s),setTimeout((function(){return t.classList.remove(s)}),2e3))}),150))},T=function(e){var t,n=null;switch(e.key){case"ArrowRight":var a=C.indexOf(e.target)+1;n=C[a]||C[0];break;case"ArrowLeft":var r=C.indexOf(e.target)-1;n=C[r]||C[C.length-1]}null==(t=n)||t.focus()};return a.createElement("div",{className:"tabs-container"},a.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.Z)("tabs",{"tabs--block":r},p)},f.map((function(e){var t=e.value,n=e.label;return a.createElement("li",{role:"tab",tabIndex:N===t?0:-1,"aria-selected":N===t,className:(0,o.Z)("tabs__item",l,{"tabs__item--active":N===t}),key:t,ref:function(e){return C.push(e)},onKeyDown:T,onFocus:P,onClick:P},null!=n?n:t)}))),n?(0,a.cloneElement)(m.filter((function(e){return e.props.value===N}))[0],{className:"margin-vert--md"}):a.createElement("div",{className:"margin-vert--md"},m.map((function(e,t){return(0,a.cloneElement)(e,{key:t,hidden:e.props.value!==N})}))))}},79443:function(e,t,n){var a=(0,n(67294).createContext)(void 0);t.Z=a},89275:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return d},contentTitle:function(){return u},metadata:function(){return c},toc:function(){return p},default:function(){return f}});var a=n(87462),r=n(63366),i=(n(67294),n(3905)),o=n(55064),l=n(58215),s=["components"],d={id:"cookbooks-deduplication",title:"Message deduplication",sidebar_label:"Message deduplication",original_id:"cookbooks-deduplication"},u=void 0,c={unversionedId:"cookbooks-deduplication",id:"version-2.6.2/cookbooks-deduplication",isDocsHomePage:!1,title:"Message deduplication",description:"When Message deduplication is enabled, it ensures that each message produced on Pulsar topics is persisted to disk only once, even if the message is produced more than once. Message deduplication is handled automatically on the server side.",source:"@site/versioned_docs/version-2.6.2/cookbooks-deduplication.md",sourceDirName:".",slug:"/cookbooks-deduplication",permalink:"/docs/2.6.2/cookbooks-deduplication",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.2/cookbooks-deduplication.md",tags:[],version:"2.6.2",frontMatter:{id:"cookbooks-deduplication",title:"Message deduplication",sidebar_label:"Message deduplication",original_id:"cookbooks-deduplication"},sidebar:"version-2.6.2/docsSidebar",previous:{title:"Topic compaction",permalink:"/docs/2.6.2/cookbooks-compaction"},next:{title:"Non-persistent messaging",permalink:"/docs/2.6.2/cookbooks-non-persistent"}},p=[{value:"How it works",id:"how-it-works",children:[]},{value:"Configure message deduplication",id:"configure-message-deduplication",children:[{value:"Set default value at the broker-level",id:"set-default-value-at-the-broker-level",children:[]},{value:"Enable message deduplication",id:"enable-message-deduplication",children:[]},{value:"Disable message deduplication",id:"disable-message-deduplication",children:[]}]},{value:"Pulsar clients",id:"pulsar-clients",children:[]}],m={toc:p};function f(e){var t=e.components,n=(0,r.Z)(e,s);return(0,i.kt)("wrapper",(0,a.Z)({},m,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"When ",(0,i.kt)("strong",{parentName:"p"},"Message deduplication")," is enabled, it ensures that each message produced on Pulsar topics is persisted to disk ",(0,i.kt)("em",{parentName:"p"},"only once"),", even if the message is produced more than once. Message deduplication is handled automatically on the server side. "),(0,i.kt)("p",null,"To use message deduplication in Pulsar, you need to configure your Pulsar brokers and clients."),(0,i.kt)("h2",{id:"how-it-works"},"How it works"),(0,i.kt)("p",null,"You can enable or disable message deduplication on a per-namespace basis. By default, it is disabled on all namespaces. You can enable it in the following ways:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Enable for all namespaces at the broker-level"),(0,i.kt)("li",{parentName:"ul"},"Enable for specific namespaces with the ",(0,i.kt)("inlineCode",{parentName:"li"},"pulsar-admin namespaces")," interface")),(0,i.kt)("h2",{id:"configure-message-deduplication"},"Configure message deduplication"),(0,i.kt)("p",null,"You can configure message deduplication in Pulsar using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-configuration#broker"},(0,i.kt)("inlineCode",{parentName:"a"},"broker.conf"))," configuration file. The following deduplication-related parameters are available."),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:"left"},"Parameter"),(0,i.kt)("th",{parentName:"tr",align:"left"},"Description"),(0,i.kt)("th",{parentName:"tr",align:"left"},"Default"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"brokerDeduplicationEnabled")),(0,i.kt)("td",{parentName:"tr",align:"left"},"Sets the default behavior for message deduplication in the Pulsar broker. If it is set to ",(0,i.kt)("inlineCode",{parentName:"td"},"true"),", message deduplication is enabled by default on all namespaces; if it is set to ",(0,i.kt)("inlineCode",{parentName:"td"},"false"),", you have to enable or disable deduplication on a per-namespace basis."),(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"false"))),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"brokerDeduplicationMaxNumberOfProducers")),(0,i.kt)("td",{parentName:"tr",align:"left"},"The maximum number of producers for which information is stored for deduplication purposes."),(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"10000"))),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"brokerDeduplicationEntriesInterval")),(0,i.kt)("td",{parentName:"tr",align:"left"},"The number of entries after which a deduplication informational snapshot is taken. A larger interval leads to fewer snapshots being taken, though this lengthens the topic recovery time (the time required for entries published after the snapshot to be replayed)."),(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"1000"))),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"brokerDeduplicationProducerInactivityTimeoutMinutes")),(0,i.kt)("td",{parentName:"tr",align:"left"},"The time of inactivity (in minutes) after which the broker discards deduplication information related to a disconnected producer."),(0,i.kt)("td",{parentName:"tr",align:"left"},(0,i.kt)("inlineCode",{parentName:"td"},"360")," (6 hours)")))),(0,i.kt)("h3",{id:"set-default-value-at-the-broker-level"},"Set default value at the broker-level"),(0,i.kt)("p",null,"By default, message deduplication is ",(0,i.kt)("em",{parentName:"p"},"disabled")," on all Pulsar namespaces. To enable it by default on all namespaces, set the ",(0,i.kt)("inlineCode",{parentName:"p"},"brokerDeduplicationEnabled")," parameter to ",(0,i.kt)("inlineCode",{parentName:"p"},"true")," and re-start the broker."),(0,i.kt)("p",null,"Even if you set the value for ",(0,i.kt)("inlineCode",{parentName:"p"},"brokerDeduplicationEnabled"),", enabling or disabling via Pulsar admin CLI overrides the default settings at the broker-level."),(0,i.kt)("h3",{id:"enable-message-deduplication"},"Enable message deduplication"),(0,i.kt)("p",null,"Though message deduplication is disabled by default at broker-level, you can enable message deduplication for specific namespaces using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#namespace-set-deduplication"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar-admin namespace set-deduplication"))," command. You can use the ",(0,i.kt)("inlineCode",{parentName:"p"},"--enable"),"/",(0,i.kt)("inlineCode",{parentName:"p"},"-e")," flag and specify the namespace. The following is an example with ",(0,i.kt)("inlineCode",{parentName:"p"},"<tenant>/<namespace>"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"\n$ bin/pulsar-admin namespaces set-deduplication \\\n  public/default \\\n  --enable # or just -e\n\n")),(0,i.kt)("h3",{id:"disable-message-deduplication"},"Disable message deduplication"),(0,i.kt)("p",null,"Even if you enable message deduplication at broker-level, you can disable message deduplication for a specific namespace using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/pulsar-admin#namespace-set-deduplication"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar-admin namespace set-deduplication"))," command. Use the ",(0,i.kt)("inlineCode",{parentName:"p"},"--disable"),"/",(0,i.kt)("inlineCode",{parentName:"p"},"-d")," flag and specify the namespace. The following is an example with ",(0,i.kt)("inlineCode",{parentName:"p"},"<tenant>/<namespace>"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"\n$ bin/pulsar-admin namespaces set-deduplication \\\n  public/default \\\n  --disable # or just -d\n\n")),(0,i.kt)("h2",{id:"pulsar-clients"},"Pulsar clients"),(0,i.kt)("p",null,"If you enable message deduplication in Pulsar brokers, you need complete the following tasks for your client producers:"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},"Specify a name for the producer."),(0,i.kt)("li",{parentName:"ol"},"Set the message timeout to ",(0,i.kt)("inlineCode",{parentName:"li"},"0")," (namely, no timeout).")),(0,i.kt)("p",null,"The instructions for Java, Python, and C++ clients are different."),(0,i.kt)(o.Z,{defaultValue:"Java clients",values:[{label:"Java clients",value:"Java clients"},{label:"Python clients",value:"Python clients"},{label:"C++ clients",value:"C++ clients"}],mdxType:"Tabs"},(0,i.kt)(l.Z,{value:"Java clients",mdxType:"TabItem"},(0,i.kt)("p",null,"To enable message deduplication on a ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/client-libraries-java#producers"},"Java producer"),", set the producer name using the ",(0,i.kt)("inlineCode",{parentName:"p"},"producerName")," setter, and set the timeout to ",(0,i.kt)("inlineCode",{parentName:"p"},"0")," using the ",(0,i.kt)("inlineCode",{parentName:"p"},"sendTimeout")," setter. "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'\nimport org.apache.pulsar.client.api.Producer;\nimport org.apache.pulsar.client.api.PulsarClient;\nimport java.util.concurrent.TimeUnit;\n\nPulsarClient pulsarClient = PulsarClient.builder()\n        .serviceUrl("pulsar://localhost:6650")\n        .build();\nProducer producer = pulsarClient.newProducer()\n        .producerName("producer-1")\n        .topic("persistent://public/default/topic-1")\n        .sendTimeout(0, TimeUnit.SECONDS)\n        .create();\n\n'))),(0,i.kt)(l.Z,{value:"Python clients",mdxType:"TabItem"},(0,i.kt)("p",null,"To enable message deduplication on a ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/client-libraries-python#producers"},"Python producer"),", set the producer name using ",(0,i.kt)("inlineCode",{parentName:"p"},"producer_name"),", and set the timeout to ",(0,i.kt)("inlineCode",{parentName:"p"},"0")," using ",(0,i.kt)("inlineCode",{parentName:"p"},"send_timeout_millis"),". "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'\nimport pulsar\n\nclient = pulsar.Client("pulsar://localhost:6650")\nproducer = client.create_producer(\n    "persistent://public/default/topic-1",\n    producer_name="producer-1",\n    send_timeout_millis=0)\n\n'))),(0,i.kt)(l.Z,{value:"C++ clients",mdxType:"TabItem"},(0,i.kt)("p",null,"To enable message deduplication on a ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.2/client-libraries-cpp#producer"},"C++ producer"),", set the producer name using ",(0,i.kt)("inlineCode",{parentName:"p"},"producer_name"),", and set the timeout to ",(0,i.kt)("inlineCode",{parentName:"p"},"0")," using ",(0,i.kt)("inlineCode",{parentName:"p"},"send_timeout_millis"),". "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-cpp"},'\n#include <pulsar/Client.h>\n\nstd::string serviceUrl = "pulsar://localhost:6650";\nstd::string topic = "persistent://some-tenant/ns1/topic-1";\nstd::string producerName = "producer-1";\n\nClient client(serviceUrl);\n\nProducerConfiguration producerConfig;\nproducerConfig.setSendTimeout(0);\nproducerConfig.setProducerName(producerName);\n\nProducer producer;\n\nResult result = client.createProducer(topic, producerConfig, producer);\n\n')))))}f.isMDXComponent=!0},86010:function(e,t,n){function a(e){var t,n,r="";if("string"==typeof e||"number"==typeof e)r+=e;else if("object"==typeof e)if(Array.isArray(e))for(t=0;t<e.length;t++)e[t]&&(n=a(e[t]))&&(r&&(r+=" "),r+=n);else for(t in e)e[t]&&(r&&(r+=" "),r+=t);return r}function r(){for(var e,t,n=0,r="";n<arguments.length;)(e=arguments[n++])&&(t=a(e))&&(r&&(r+=" "),r+=t);return r}n.d(t,{Z:function(){return r}})}}]);
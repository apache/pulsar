"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[14219],{3905:function(t,e,n){n.d(e,{Zo:function(){return m},kt:function(){return c}});var a=n(67294);function r(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function l(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(t);e&&(a=a.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,a)}return n}function i(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?l(Object(n),!0).forEach((function(e){r(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}function o(t,e){if(null==t)return{};var n,a,r=function(t,e){if(null==t)return{};var n,a,r={},l=Object.keys(t);for(a=0;a<l.length;a++)n=l[a],e.indexOf(n)>=0||(r[n]=t[n]);return r}(t,e);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(t);for(a=0;a<l.length;a++)n=l[a],e.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(t,n)&&(r[n]=t[n])}return r}var u=a.createContext({}),s=function(t){var e=a.useContext(u),n=e;return t&&(n="function"==typeof t?t(e):i(i({},e),t)),n},m=function(t){var e=s(t.components);return a.createElement(u.Provider,{value:e},t.children)},p={inlineCode:"code",wrapper:function(t){var e=t.children;return a.createElement(a.Fragment,{},e)}},d=a.forwardRef((function(t,e){var n=t.components,r=t.mdxType,l=t.originalType,u=t.parentName,m=o(t,["components","mdxType","originalType","parentName"]),d=s(n),c=r,g=d["".concat(u,".").concat(c)]||d[c]||p[c]||l;return n?a.createElement(g,i(i({ref:e},m),{},{components:n})):a.createElement(g,i({ref:e},m))}));function c(t,e){var n=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var l=n.length,i=new Array(l);i[0]=d;var o={};for(var u in e)hasOwnProperty.call(e,u)&&(o[u]=e[u]);o.originalType=t,o.mdxType="string"==typeof t?t:r,i[1]=o;for(var s=2;s<l;s++)i[s]=n[s];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},58215:function(t,e,n){var a=n(67294);e.Z=function(t){var e=t.children,n=t.hidden,r=t.className;return a.createElement("div",{role:"tabpanel",hidden:n,className:r},e)}},55064:function(t,e,n){n.d(e,{Z:function(){return s}});var a=n(67294),r=n(79443);var l=function(){var t=(0,a.useContext)(r.Z);if(null==t)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return t},i=n(86010),o="tabItem_1uMI",u="tabItemActive_2DSg";var s=function(t){var e,n=t.lazy,r=t.block,s=t.defaultValue,m=t.values,p=t.groupId,d=t.className,c=a.Children.toArray(t.children),g=null!=m?m:c.map((function(t){return{value:t.props.value,label:t.props.label}})),k=null!=s?s:null==(e=c.find((function(t){return t.props.default})))?void 0:e.props.value,b=l(),f=b.tabGroupChoices,N=b.setTabGroupChoices,h=(0,a.useState)(k),v=h[0],y=h[1],C=[];if(null!=p){var w=f[p];null!=w&&w!==v&&g.some((function(t){return t.value===w}))&&y(w)}var T=function(t){var e=t.currentTarget,n=C.indexOf(e),a=g[n].value;y(a),null!=p&&(N(p,a),setTimeout((function(){var t,n,a,r,l,i,o,s;(t=e.getBoundingClientRect(),n=t.top,a=t.left,r=t.bottom,l=t.right,i=window,o=i.innerHeight,s=i.innerWidth,n>=0&&l<=s&&r<=o&&a>=0)||(e.scrollIntoView({block:"center",behavior:"smooth"}),e.classList.add(u),setTimeout((function(){return e.classList.remove(u)}),2e3))}),150))},x=function(t){var e,n=null;switch(t.key){case"ArrowRight":var a=C.indexOf(t.target)+1;n=C[a]||C[0];break;case"ArrowLeft":var r=C.indexOf(t.target)-1;n=C[r]||C[C.length-1]}null==(e=n)||e.focus()};return a.createElement("div",{className:"tabs-container"},a.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":r},d)},g.map((function(t){var e=t.value,n=t.label;return a.createElement("li",{role:"tab",tabIndex:v===e?0:-1,"aria-selected":v===e,className:(0,i.Z)("tabs__item",o,{"tabs__item--active":v===e}),key:e,ref:function(t){return C.push(t)},onKeyDown:x,onFocus:T,onClick:T},null!=n?n:e)}))),n?(0,a.cloneElement)(c.filter((function(t){return t.props.value===v}))[0],{className:"margin-vert--md"}):a.createElement("div",{className:"margin-vert--md"},c.map((function(t,e){return(0,a.cloneElement)(t,{key:e,hidden:t.props.value!==v})}))))}},79443:function(t,e,n){var a=(0,n(67294).createContext)(void 0);e.Z=a},5401:function(t,e,n){n.r(e),n.d(e,{frontMatter:function(){return o},contentTitle:function(){return u},metadata:function(){return s},toc:function(){return m},default:function(){return d}});var a=n(87462),r=n(63366),l=(n(67294),n(3905)),i=(n(55064),n(58215),["components"]),o={id:"io-rabbitmq-source",title:"RabbitMQ source connector",sidebar_label:"RabbitMQ source connector",original_id:"io-rabbitmq-source"},u=void 0,s={unversionedId:"io-rabbitmq-source",id:"version-2.7.0/io-rabbitmq-source",isDocsHomePage:!1,title:"RabbitMQ source connector",description:"The RabbitMQ source connector receives messages from RabbitMQ clusters",source:"@site/versioned_docs/version-2.7.0/io-rabbitmq-source.md",sourceDirName:".",slug:"/io-rabbitmq-source",permalink:"/docs/2.7.0/io-rabbitmq-source",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.0/io-rabbitmq-source.md",tags:[],version:"2.7.0",frontMatter:{id:"io-rabbitmq-source",title:"RabbitMQ source connector",sidebar_label:"RabbitMQ source connector",original_id:"io-rabbitmq-source"}},m=[{value:"Configuration",id:"configuration",children:[{value:"Property",id:"property",children:[]},{value:"Example",id:"example",children:[]}]}],p={toc:m};function d(t){var e=t.components,n=(0,r.Z)(t,i);return(0,l.kt)("wrapper",(0,a.Z)({},p,n,{components:e,mdxType:"MDXLayout"}),(0,l.kt)("p",null,"The RabbitMQ source connector receives messages from RabbitMQ clusters\nand writes messages to Pulsar topics."),(0,l.kt)("h2",{id:"configuration"},"Configuration"),(0,l.kt)("p",null,"The configuration of the RabbitMQ source connector has the following properties."),(0,l.kt)("h3",{id:"property"},"Property"),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Name"),(0,l.kt)("th",{parentName:"tr",align:null},"Type"),(0,l.kt)("th",{parentName:"tr",align:null},"Required"),(0,l.kt)("th",{parentName:"tr",align:null},"Default"),(0,l.kt)("th",{parentName:"tr",align:null},"Description"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"connectionName")),(0,l.kt)("td",{parentName:"tr",align:null},"String"),(0,l.kt)("td",{parentName:"tr",align:null},"true"),(0,l.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,l.kt)("td",{parentName:"tr",align:null},"The connection name.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"host")),(0,l.kt)("td",{parentName:"tr",align:null},"String"),(0,l.kt)("td",{parentName:"tr",align:null},"true"),(0,l.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,l.kt)("td",{parentName:"tr",align:null},"The RabbitMQ host.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"port")),(0,l.kt)("td",{parentName:"tr",align:null},"int"),(0,l.kt)("td",{parentName:"tr",align:null},"true"),(0,l.kt)("td",{parentName:"tr",align:null},"5672"),(0,l.kt)("td",{parentName:"tr",align:null},"The RabbitMQ port.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"virtualHost")),(0,l.kt)("td",{parentName:"tr",align:null},"String"),(0,l.kt)("td",{parentName:"tr",align:null},"true"),(0,l.kt)("td",{parentName:"tr",align:null},"/"),(0,l.kt)("td",{parentName:"tr",align:null},"The virtual host used to connect to RabbitMQ.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"username")),(0,l.kt)("td",{parentName:"tr",align:null},"String"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"guest"),(0,l.kt)("td",{parentName:"tr",align:null},"The username used to authenticate to RabbitMQ.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"password")),(0,l.kt)("td",{parentName:"tr",align:null},"String"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"guest"),(0,l.kt)("td",{parentName:"tr",align:null},"The password used to authenticate to RabbitMQ.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"queueName")),(0,l.kt)("td",{parentName:"tr",align:null},"String"),(0,l.kt)("td",{parentName:"tr",align:null},"true"),(0,l.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,l.kt)("td",{parentName:"tr",align:null},"The RabbitMQ queue name that messages should be read from or written to.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"requestedChannelMax")),(0,l.kt)("td",{parentName:"tr",align:null},"int"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"0"),(0,l.kt)("td",{parentName:"tr",align:null},"The initially requested maximum channel number. ",(0,l.kt)("br",null),(0,l.kt)("br",null),"0 means unlimited.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"requestedFrameMax")),(0,l.kt)("td",{parentName:"tr",align:null},"int"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"0"),(0,l.kt)("td",{parentName:"tr",align:null},"The initially requested maximum frame size in octets. ",(0,l.kt)("br",null),(0,l.kt)("br",null),"0 means unlimited.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"connectionTimeout")),(0,l.kt)("td",{parentName:"tr",align:null},"int"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"60000"),(0,l.kt)("td",{parentName:"tr",align:null},"The timeout of TCP connection establishment in milliseconds. ",(0,l.kt)("br",null),(0,l.kt)("br",null),"0 means infinite.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"handshakeTimeout")),(0,l.kt)("td",{parentName:"tr",align:null},"int"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"10000"),(0,l.kt)("td",{parentName:"tr",align:null},"The timeout of AMQP0-9-1 protocol handshake in milliseconds.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"requestedHeartbeat")),(0,l.kt)("td",{parentName:"tr",align:null},"int"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"60"),(0,l.kt)("td",{parentName:"tr",align:null},"The requested heartbeat timeout in seconds.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"prefetchCount")),(0,l.kt)("td",{parentName:"tr",align:null},"int"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"0"),(0,l.kt)("td",{parentName:"tr",align:null},"The maximum number of messages that the server delivers.",(0,l.kt)("br",null),(0,l.kt)("br",null)," 0 means unlimited.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"prefetchGlobal")),(0,l.kt)("td",{parentName:"tr",align:null},"boolean"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"Whether the setting should be applied to the entire channel rather than each consumer.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"passive")),(0,l.kt)("td",{parentName:"tr",align:null},"boolean"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"false"),(0,l.kt)("td",{parentName:"tr",align:null},"Whether the rabbitmq consumer should create its own queue or bind to an existing one.")))),(0,l.kt)("h3",{id:"example"},"Example"),(0,l.kt)("p",null,"Before using the RabbitMQ source connector, you need to create a configuration file through one of the following methods."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"JSON "),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n    "host": "localhost",\n    "port": "5672",\n    "virtualHost": "/",\n    "username": "guest",\n    "password": "guest",\n    "queueName": "test-queue",\n    "connectionName": "test-connection",\n    "requestedChannelMax": "0",\n    "requestedFrameMax": "0",\n    "connectionTimeout": "60000",\n    "handshakeTimeout": "10000",\n    "requestedHeartbeat": "60",\n    "prefetchCount": "0",\n    "prefetchGlobal": "false",\n    "passive": "false"\n}\n\n'))),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"YAML"),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-yaml"},'\nconfigs:\n    host: "localhost"\n    port: 5672\n    virtualHost: "/"\n    username: "guest"\n    password: "guest"\n    queueName: "test-queue"\n    connectionName: "test-connection"\n    requestedChannelMax: 0\n    requestedFrameMax: 0\n    connectionTimeout: 60000\n    handshakeTimeout: 10000\n    requestedHeartbeat: 60\n    prefetchCount: 0\n    prefetchGlobal: "false"\n    passive: "false"\n\n')))))}d.isMDXComponent=!0},86010:function(t,e,n){function a(t){var e,n,r="";if("string"==typeof t||"number"==typeof t)r+=t;else if("object"==typeof t)if(Array.isArray(t))for(e=0;e<t.length;e++)t[e]&&(n=a(t[e]))&&(r&&(r+=" "),r+=n);else for(e in t)t[e]&&(r&&(r+=" "),r+=e);return r}function r(){for(var t,e,n=0,r="";n<arguments.length;)(t=arguments[n++])&&(e=a(t))&&(r&&(r+=" "),r+=e);return r}n.d(e,{Z:function(){return r}})}}]);
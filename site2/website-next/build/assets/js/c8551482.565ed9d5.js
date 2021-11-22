"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[11365],{3905:function(t,e,n){n.d(e,{Zo:function(){return s},kt:function(){return g}});var r=n(67294);function a(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function o(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function i(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?o(Object(n),!0).forEach((function(e){a(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}function l(t,e){if(null==t)return{};var n,r,a=function(t,e){if(null==t)return{};var n,r,a={},o=Object.keys(t);for(r=0;r<o.length;r++)n=o[r],e.indexOf(n)>=0||(a[n]=t[n]);return a}(t,e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(t);for(r=0;r<o.length;r++)n=o[r],e.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(t,n)&&(a[n]=t[n])}return a}var c=r.createContext({}),p=function(t){var e=r.useContext(c),n=e;return t&&(n="function"==typeof t?t(e):i(i({},e),t)),n},s=function(t){var e=p(t.components);return r.createElement(c.Provider,{value:e},t.children)},m={inlineCode:"code",wrapper:function(t){var e=t.children;return r.createElement(r.Fragment,{},e)}},u=r.forwardRef((function(t,e){var n=t.components,a=t.mdxType,o=t.originalType,c=t.parentName,s=l(t,["components","mdxType","originalType","parentName"]),u=p(n),g=a,d=u["".concat(c,".").concat(g)]||u[g]||m[g]||o;return n?r.createElement(d,i(i({ref:e},s),{},{components:n})):r.createElement(d,i({ref:e},s))}));function g(t,e){var n=arguments,a=e&&e.mdxType;if("string"==typeof t||a){var o=n.length,i=new Array(o);i[0]=u;var l={};for(var c in e)hasOwnProperty.call(e,c)&&(l[c]=e[c]);l.originalType=t,l.mdxType="string"==typeof t?t:a,i[1]=l;for(var p=2;p<o;p++)i[p]=n[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}u.displayName="MDXCreateElement"},71162:function(t,e,n){n.r(e),n.d(e,{frontMatter:function(){return l},contentTitle:function(){return c},metadata:function(){return p},toc:function(){return s},default:function(){return u}});var r=n(87462),a=n(63366),o=(n(67294),n(3905)),i=["components"],l={id:"io-mongo-sink",title:"MongoDB sink connector",sidebar_label:"MongoDB sink connector",original_id:"io-mongo-sink"},c=void 0,p={unversionedId:"io-mongo-sink",id:"version-2.6.4/io-mongo-sink",isDocsHomePage:!1,title:"MongoDB sink connector",description:"The MongoDB sink connector pulls messages from Pulsar topics",source:"@site/versioned_docs/version-2.6.4/io-mongo-sink.md",sourceDirName:".",slug:"/io-mongo-sink",permalink:"/docs/2.6.4/io-mongo-sink",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.4/io-mongo-sink.md",tags:[],version:"2.6.4",frontMatter:{id:"io-mongo-sink",title:"MongoDB sink connector",sidebar_label:"MongoDB sink connector",original_id:"io-mongo-sink"}},s=[{value:"Configuration",id:"configuration",children:[{value:"Property",id:"property",children:[]},{value:"Example",id:"example",children:[]}]}],m={toc:s};function u(t){var e=t.components,n=(0,a.Z)(t,i);return(0,o.kt)("wrapper",(0,r.Z)({},m,n,{components:e,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"The MongoDB sink connector pulls messages from Pulsar topics\nand persists the messages to collections."),(0,o.kt)("h2",{id:"configuration"},"Configuration"),(0,o.kt)("p",null,"The configuration of the MongoDB sink connector has the following properties."),(0,o.kt)("h3",{id:"property"},"Property"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Name"),(0,o.kt)("th",{parentName:"tr",align:null},"Type"),(0,o.kt)("th",{parentName:"tr",align:null},"Required"),(0,o.kt)("th",{parentName:"tr",align:null},"Default"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"mongoUri")),(0,o.kt)("td",{parentName:"tr",align:null},"String"),(0,o.kt)("td",{parentName:"tr",align:null},"true"),(0,o.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,o.kt)("td",{parentName:"tr",align:null},"The MongoDB URI to which the connector connects. ",(0,o.kt)("br",null),(0,o.kt)("br",null),"For more information, see ",(0,o.kt)("a",{parentName:"td",href:"https://docs.mongodb.com/manual/reference/connection-string/"},"connection string URI format"),".")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"database")),(0,o.kt)("td",{parentName:"tr",align:null},"String"),(0,o.kt)("td",{parentName:"tr",align:null},"true"),(0,o.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,o.kt)("td",{parentName:"tr",align:null},"The database name to which the collection belongs.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"collection")),(0,o.kt)("td",{parentName:"tr",align:null},"String"),(0,o.kt)("td",{parentName:"tr",align:null},"true"),(0,o.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,o.kt)("td",{parentName:"tr",align:null},"The collection name to which the connector writes messages.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"batchSize")),(0,o.kt)("td",{parentName:"tr",align:null},"int"),(0,o.kt)("td",{parentName:"tr",align:null},"false"),(0,o.kt)("td",{parentName:"tr",align:null},"100"),(0,o.kt)("td",{parentName:"tr",align:null},"The batch size of writing messages to collections.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"batchTimeMs")),(0,o.kt)("td",{parentName:"tr",align:null},"long"),(0,o.kt)("td",{parentName:"tr",align:null},"false"),(0,o.kt)("td",{parentName:"tr",align:null},"1000"),(0,o.kt)("td",{parentName:"tr",align:null},"The batch operation interval in milliseconds.")))),(0,o.kt)("h3",{id:"example"},"Example"),(0,o.kt)("p",null,"Before using the Mongo sink connector, you need to create a configuration file through one of the following methods."),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"JSON"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n    "mongoUri": "mongodb://localhost:27017",\n    "database": "pulsar",\n    "collection": "messages",\n    "batchSize": "2",\n    "batchTimeMs": "500"\n}\n\n'))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"YAML"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},'\n{\n    mongoUri: "mongodb://localhost:27017"\n    database: "pulsar"\n    collection: "messages"\n    batchSize: 2\n    batchTimeMs: 500\n}\n\n')))))}u.isMDXComponent=!0}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[84295],{3905:function(e,t,n){n.d(t,{Zo:function(){return u},kt:function(){return d}});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function s(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var c=r.createContext({}),l=function(e){var t=r.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):s(s({},t),e)),n},u=function(e){var t=l(e.components);return r.createElement(c.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,c=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),m=l(n),d=a,k=m["".concat(c,".").concat(d)]||m[d]||p[d]||o;return n?r.createElement(k,s(s({ref:t},u),{},{components:n})):r.createElement(k,s({ref:t},u))}));function d(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,s=new Array(o);s[0]=m;var i={};for(var c in t)hasOwnProperty.call(t,c)&&(i[c]=t[c]);i.originalType=e,i.mdxType="string"==typeof e?e:a,s[1]=i;for(var l=2;l<o;l++)s[l]=n[l];return r.createElement.apply(null,s)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},58215:function(e,t,n){var r=n(67294);t.Z=function(e){var t=e.children,n=e.hidden,a=e.className;return r.createElement("div",{role:"tabpanel",hidden:n,className:a},t)}},55064:function(e,t,n){n.d(t,{Z:function(){return l}});var r=n(67294),a=n(79443);var o=function(){var e=(0,r.useContext)(a.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},s=n(86010),i="tabItem_1uMI",c="tabItemActive_2DSg";var l=function(e){var t,n=e.lazy,a=e.block,l=e.defaultValue,u=e.values,p=e.groupId,m=e.className,d=r.Children.toArray(e.children),k=null!=u?u:d.map((function(e){return{value:e.props.value,label:e.props.label}})),f=null!=l?l:null==(t=d.find((function(e){return e.props.default})))?void 0:t.props.value,h=o(),g=h.tabGroupChoices,v=h.setTabGroupChoices,b=(0,r.useState)(f),N=b[0],y=b[1],w=[];if(null!=p){var O=g[p];null!=O&&O!==N&&k.some((function(e){return e.value===O}))&&y(O)}var C=function(e){var t=e.currentTarget,n=w.indexOf(t),r=k[n].value;y(r),null!=p&&(v(p,r),setTimeout((function(){var e,n,r,a,o,s,i,l;(e=t.getBoundingClientRect(),n=e.top,r=e.left,a=e.bottom,o=e.right,s=window,i=s.innerHeight,l=s.innerWidth,n>=0&&o<=l&&a<=i&&r>=0)||(t.scrollIntoView({block:"center",behavior:"smooth"}),t.classList.add(c),setTimeout((function(){return t.classList.remove(c)}),2e3))}),150))},E=function(e){var t,n=null;switch(e.key){case"ArrowRight":var r=w.indexOf(e.target)+1;n=w[r]||w[0];break;case"ArrowLeft":var a=w.indexOf(e.target)-1;n=w[a]||w[w.length-1]}null==(t=n)||t.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,s.Z)("tabs",{"tabs--block":a},m)},k.map((function(e){var t=e.value,n=e.label;return r.createElement("li",{role:"tab",tabIndex:N===t?0:-1,"aria-selected":N===t,className:(0,s.Z)("tabs__item",i,{"tabs__item--active":N===t}),key:t,ref:function(e){return w.push(e)},onKeyDown:E,onFocus:C,onClick:C},null!=n?n:t)}))),n?(0,r.cloneElement)(d.filter((function(e){return e.props.value===N}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},d.map((function(e,t){return(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==N})}))))}},79443:function(e,t,n){var r=(0,n(67294).createContext)(void 0);t.Z=r},5571:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return l},contentTitle:function(){return u},metadata:function(){return p},toc:function(){return m},default:function(){return k}});var r=n(87462),a=n(63366),o=(n(67294),n(3905)),s=n(55064),i=n(58215),c=["components"],l={id:"io-overview",title:"Pulsar connector overview",sidebar_label:"Overview",original_id:"io-overview"},u=void 0,p={unversionedId:"io-overview",id:"version-2.7.3/io-overview",isDocsHomePage:!1,title:"Pulsar connector overview",description:"Messaging systems are most powerful when you can easily use them with external systems like databases and other messaging systems.",source:"@site/versioned_docs/version-2.7.3/io-overview.md",sourceDirName:".",slug:"/io-overview",permalink:"/docs/2.7.3/io-overview",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.3/io-overview.md",tags:[],version:"2.7.3",frontMatter:{id:"io-overview",title:"Pulsar connector overview",sidebar_label:"Overview",original_id:"io-overview"},sidebar:"version-2.7.3/docsSidebar",previous:{title:"Window Functions: Context",permalink:"/docs/2.7.3/window-functions-context"},next:{title:"Get started",permalink:"/docs/2.7.3/io-quickstart"}},m=[{value:"Concept",id:"concept",children:[{value:"Source",id:"source",children:[]},{value:"Sink",id:"sink",children:[]}]},{value:"Processing guarantee",id:"processing-guarantee",children:[{value:"Set",id:"set",children:[]},{value:"Update",id:"update",children:[]}]},{value:"Work with connector",id:"work-with-connector",children:[]}],d={toc:m};function k(e){var t=e.components,l=(0,a.Z)(e,c);return(0,o.kt)("wrapper",(0,r.Z)({},d,l,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"Messaging systems are most powerful when you can easily use them with external systems like databases and other messaging systems."),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Pulsar IO connectors")," enable you to easily create, deploy, and manage connectors that interact with external systems, such as ",(0,o.kt)("a",{parentName:"p",href:"https://cassandra.apache.org"},"Apache Cassandra"),", ",(0,o.kt)("a",{parentName:"p",href:"https://www.aerospike.com"},"Aerospike"),", and many others."),(0,o.kt)("h2",{id:"concept"},"Concept"),(0,o.kt)("p",null,"Pulsar IO connectors come in two types: ",(0,o.kt)("strong",{parentName:"p"},"source")," and ",(0,o.kt)("strong",{parentName:"p"},"sink"),"."),(0,o.kt)("p",null,"This diagram illustrates the relationship between source, Pulsar, and sink:"),(0,o.kt)("p",null,(0,o.kt)("img",{alt:"Pulsar IO diagram",src:n(89103).Z,title:"Pulsar IO connectors (sources and sinks)"})),(0,o.kt)("h3",{id:"source"},"Source"),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"Sources ",(0,o.kt)("strong",{parentName:"p"},"feed data from external systems into Pulsar"),".")),(0,o.kt)("p",null,"Common sources include other messaging systems and firehose-style data pipeline APIs."),(0,o.kt)("p",null,"For the complete list of Pulsar built-in source connectors, see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/io-connectors#source-connector"},"source connector"),"."),(0,o.kt)("h3",{id:"sink"},"Sink"),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"Sinks ",(0,o.kt)("strong",{parentName:"p"},"feed data from Pulsar into external systems"),".")),(0,o.kt)("p",null,"Common sinks include other messaging systems and SQL and NoSQL databases."),(0,o.kt)("p",null,"For the complete list of Pulsar built-in sink connectors, see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/io-connectors#sink-connector"},"sink connector"),"."),(0,o.kt)("h2",{id:"processing-guarantee"},"Processing guarantee"),(0,o.kt)("p",null,"Processing guarantees are used to handle errors when writing messages to Pulsar topics."),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"Pulsar connectors and Functions use the ",(0,o.kt)("strong",{parentName:"p"},"same")," processing guarantees as below.")),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:"left"},"Delivery semantic"),(0,o.kt)("th",{parentName:"tr",align:"left"},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:"left"},(0,o.kt)("inlineCode",{parentName:"td"},"at-most-once")),(0,o.kt)("td",{parentName:"tr",align:"left"},"Each message sent to a connector is to be ",(0,o.kt)("strong",{parentName:"td"},"processed once")," or ",(0,o.kt)("strong",{parentName:"td"},"not to be processed"),".")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:"left"},(0,o.kt)("inlineCode",{parentName:"td"},"at-least-once")),(0,o.kt)("td",{parentName:"tr",align:"left"},"Each message sent to a connector is to be ",(0,o.kt)("strong",{parentName:"td"},"processed once")," or ",(0,o.kt)("strong",{parentName:"td"},"more than once"),".")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:"left"},(0,o.kt)("inlineCode",{parentName:"td"},"effectively-once")),(0,o.kt)("td",{parentName:"tr",align:"left"},"Each message sent to a connector has ",(0,o.kt)("strong",{parentName:"td"},"one output associated")," with it.")))),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"Processing guarantees for connectors not just rely on Pulsar guarantee but also ",(0,o.kt)("strong",{parentName:"p"},"relate to external systems"),", that is, ",(0,o.kt)("strong",{parentName:"p"},"the implementation of source and sink"),".")),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Source: Pulsar ensures that writing messages to Pulsar topics respects to the processing guarantees. It is within Pulsar's control.")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Sink: the processing guarantees rely on the sink implementation. If the sink implementation does not handle retries in an idempotent way, the sink does not respect to the processing guarantees."))),(0,o.kt)("h3",{id:"set"},"Set"),(0,o.kt)("p",null,"When creating a connector, you can set the processing guarantee with the following semantics:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"ATLEAST_ONCE"),(0,o.kt)("li",{parentName:"ul"},"ATMOST_ONCE"),(0,o.kt)("li",{parentName:"ul"},"EFFECTIVELY_ONCE")),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"If ",(0,o.kt)("inlineCode",{parentName:"p"},"--processing-guarantees")," is not specified when creating a connector, the default semantic is ",(0,o.kt)("inlineCode",{parentName:"p"},"ATLEAST_ONCE"),".")),(0,o.kt)("p",null,"Here takes ",(0,o.kt)("strong",{parentName:"p"},"Admin CLI")," as an example. For more information about ",(0,o.kt)("strong",{parentName:"p"},"REST API")," or ",(0,o.kt)("strong",{parentName:"p"},"JAVA Admin API"),", see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/io-use#create"},"here"),". "),(0,o.kt)(s.Z,{defaultValue:"Source",values:[{label:"Source",value:"Source"},{label:"Sink",value:"Sink"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"Source",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\n$ bin/pulsar-admin sources create \\\n  --processing-guarantees ATMOST_ONCE \\\n  # Other source configs\n\n")),(0,o.kt)("p",null,"For more information about the options of ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin sources create"),", see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/reference-connector-admin#create"},"here"),".")),(0,o.kt)(i.Z,{value:"Sink",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\n$ bin/pulsar-admin sinks create \\\n  --processing-guarantees EFFECTIVELY_ONCE \\\n  # Other sink configs\n\n")),(0,o.kt)("p",null,"For more information about the options of ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin sinks create"),", see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/reference-connector-admin#create-1"},"here"),"."))),(0,o.kt)("h3",{id:"update"},"Update"),(0,o.kt)("p",null,"After creating a connector, you can update the processing guarantee with the following semantics:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"ATLEAST_ONCE"),(0,o.kt)("li",{parentName:"ul"},"ATMOST_ONCE"),(0,o.kt)("li",{parentName:"ul"},"EFFECTIVELY_ONCE")),(0,o.kt)("p",null,"Here takes ",(0,o.kt)("strong",{parentName:"p"},"Admin CLI")," as an example. For more information about ",(0,o.kt)("strong",{parentName:"p"},"REST API")," or ",(0,o.kt)("strong",{parentName:"p"},"JAVA Admin API"),", see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/io-use#create"},"here"),". "),(0,o.kt)(s.Z,{defaultValue:"Source",values:[{label:"Source",value:"Source"},{label:"Sink",value:"Sink"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"Source",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\n$ bin/pulsar-admin sources update \\\n  --processing-guarantees EFFECTIVELY_ONCE \\\n  # Other source configs\n\n")),(0,o.kt)("p",null,"For more information about the options of ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin sources update"),", see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/reference-connector-admin#update"},"here"),".")),(0,o.kt)(i.Z,{value:"Sink",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\n$ bin/pulsar-admin sinks update \\\n  --processing-guarantees ATMOST_ONCE \\\n  # Other sink configs\n\n")),(0,o.kt)("p",null,"For more information about the options of ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin sinks update"),", see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/reference-connector-admin#update-1"},"here"),"."))),(0,o.kt)("h2",{id:"work-with-connector"},"Work with connector"),(0,o.kt)("p",null,"You can manage Pulsar connectors (for example, create, update, start, stop, restart, reload, delete and perform other operations on connectors) via the ",(0,o.kt)("a",{parentName:"p",href:"reference-connector-admin"},"Connector Admin CLI")," with ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/reference-connector-admin#sources"},"sources")," and ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/reference-connector-admin#sinks"},"sinks")," subcommands."),(0,o.kt)("p",null,"Connectors (sources and sinks) and Functions are components of instances, and they all run on Functions workers. When managing a source, sink or function via ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/reference-connector-admin"},"Connector Admin CLI")," or ",(0,o.kt)("a",{parentName:"p",href:"functions-cli"},"Functions Admin CLI"),", an instance is started on a worker. For more information, see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.3/functions-worker#run-functions-worker-separately"},"Functions worker"),"."))}k.isMDXComponent=!0},86010:function(e,t,n){function r(e){var t,n,a="";if("string"==typeof e||"number"==typeof e)a+=e;else if("object"==typeof e)if(Array.isArray(e))for(t=0;t<e.length;t++)e[t]&&(n=r(e[t]))&&(a&&(a+=" "),a+=n);else for(t in e)e[t]&&(a&&(a+=" "),a+=t);return a}function a(){for(var e,t,n=0,a="";n<arguments.length;)(e=arguments[n++])&&(t=r(e))&&(a&&(a+=" "),a+=t);return a}n.d(t,{Z:function(){return a}})},89103:function(e,t,n){t.Z=n.p+"assets/images/pulsar-io-8e834df5eaed9d5b0a7e0ffa162e850a.png"}}]);
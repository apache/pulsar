"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[57536],{3905:function(t,e,n){n.d(e,{Zo:function(){return c},kt:function(){return d}});var r=n(67294);function l(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function a(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function o(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?a(Object(n),!0).forEach((function(e){l(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}function i(t,e){if(null==t)return{};var n,r,l=function(t,e){if(null==t)return{};var n,r,l={},a=Object.keys(t);for(r=0;r<a.length;r++)n=a[r],e.indexOf(n)>=0||(l[n]=t[n]);return l}(t,e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(t);for(r=0;r<a.length;r++)n=a[r],e.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(t,n)&&(l[n]=t[n])}return l}var s=r.createContext({}),u=function(t){var e=r.useContext(s),n=e;return t&&(n="function"==typeof t?t(e):o(o({},e),t)),n},c=function(t){var e=u(t.components);return r.createElement(s.Provider,{value:e},t.children)},p={inlineCode:"code",wrapper:function(t){var e=t.children;return r.createElement(r.Fragment,{},e)}},m=r.forwardRef((function(t,e){var n=t.components,l=t.mdxType,a=t.originalType,s=t.parentName,c=i(t,["components","mdxType","originalType","parentName"]),m=u(n),d=l,k=m["".concat(s,".").concat(d)]||m[d]||p[d]||a;return n?r.createElement(k,o(o({ref:e},c),{},{components:n})):r.createElement(k,o({ref:e},c))}));function d(t,e){var n=arguments,l=e&&e.mdxType;if("string"==typeof t||l){var a=n.length,o=new Array(a);o[0]=m;var i={};for(var s in e)hasOwnProperty.call(e,s)&&(i[s]=e[s]);i.originalType=t,i.mdxType="string"==typeof t?t:l,o[1]=i;for(var u=2;u<a;u++)o[u]=n[u];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},58215:function(t,e,n){var r=n(67294);e.Z=function(t){var e=t.children,n=t.hidden,l=t.className;return r.createElement("div",{role:"tabpanel",hidden:n,className:l},e)}},55064:function(t,e,n){n.d(e,{Z:function(){return u}});var r=n(67294),l=n(79443);var a=function(){var t=(0,r.useContext)(l.Z);if(null==t)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return t},o=n(86010),i="tabItem_1uMI",s="tabItemActive_2DSg";var u=function(t){var e,n=t.lazy,l=t.block,u=t.defaultValue,c=t.values,p=t.groupId,m=t.className,d=r.Children.toArray(t.children),k=null!=c?c:d.map((function(t){return{value:t.props.value,label:t.props.label}})),f=null!=u?u:null==(e=d.find((function(t){return t.props.default})))?void 0:e.props.value,g=a(),h=g.tabGroupChoices,b=g.setTabGroupChoices,N=(0,r.useState)(f),v=N[0],y=N[1],w=[];if(null!=p){var C=h[p];null!=C&&C!==v&&k.some((function(t){return t.value===C}))&&y(C)}var S=function(t){var e=t.currentTarget,n=w.indexOf(e),r=k[n].value;y(r),null!=p&&(b(p,r),setTimeout((function(){var t,n,r,l,a,o,i,u;(t=e.getBoundingClientRect(),n=t.top,r=t.left,l=t.bottom,a=t.right,o=window,i=o.innerHeight,u=o.innerWidth,n>=0&&a<=u&&l<=i&&r>=0)||(e.scrollIntoView({block:"center",behavior:"smooth"}),e.classList.add(s),setTimeout((function(){return e.classList.remove(s)}),2e3))}),150))},O=function(t){var e,n=null;switch(t.key){case"ArrowRight":var r=w.indexOf(t.target)+1;n=w[r]||w[0];break;case"ArrowLeft":var l=w.indexOf(t.target)-1;n=w[l]||w[w.length-1]}null==(e=n)||e.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.Z)("tabs",{"tabs--block":l},m)},k.map((function(t){var e=t.value,n=t.label;return r.createElement("li",{role:"tab",tabIndex:v===e?0:-1,"aria-selected":v===e,className:(0,o.Z)("tabs__item",i,{"tabs__item--active":v===e}),key:e,ref:function(t){return w.push(t)},onKeyDown:O,onFocus:S,onClick:S},null!=n?n:e)}))),n?(0,r.cloneElement)(d.filter((function(t){return t.props.value===v}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},d.map((function(t,e){return(0,r.cloneElement)(t,{key:e,hidden:t.props.value!==v})}))))}},79443:function(t,e,n){var r=(0,n(67294).createContext)(void 0);e.Z=r},54210:function(t,e,n){n.r(e),n.d(e,{frontMatter:function(){return i},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return c},default:function(){return m}});var r=n(87462),l=n(63366),a=(n(67294),n(3905)),o=(n(55064),n(58215),["components"]),i={id:"io-solr-sink",title:"Solr sink connector",sidebar_label:"Solr sink connector",original_id:"io-solr-sink"},s=void 0,u={unversionedId:"io-solr-sink",id:"version-2.7.0/io-solr-sink",isDocsHomePage:!1,title:"Solr sink connector",description:"The Solr sink connector pulls messages from Pulsar topics",source:"@site/versioned_docs/version-2.7.0/io-solr-sink.md",sourceDirName:".",slug:"/io-solr-sink",permalink:"/docs/2.7.0/io-solr-sink",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.0/io-solr-sink.md",tags:[],version:"2.7.0",frontMatter:{id:"io-solr-sink",title:"Solr sink connector",sidebar_label:"Solr sink connector",original_id:"io-solr-sink"}},c=[{value:"Configuration",id:"configuration",children:[{value:"Property",id:"property",children:[]},{value:"Example",id:"example",children:[]}]}],p={toc:c};function m(t){var e=t.components,n=(0,l.Z)(t,o);return(0,a.kt)("wrapper",(0,r.Z)({},p,n,{components:e,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"The Solr sink connector pulls messages from Pulsar topics\nand persists the messages to Solr collections."),(0,a.kt)("h2",{id:"configuration"},"Configuration"),(0,a.kt)("p",null,"The configuration of the Solr sink connector has the following properties."),(0,a.kt)("h3",{id:"property"},"Property"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Name"),(0,a.kt)("th",{parentName:"tr",align:null},"Type"),(0,a.kt)("th",{parentName:"tr",align:null},"Required"),(0,a.kt)("th",{parentName:"tr",align:null},"Default"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"solrUrl")),(0,a.kt)("td",{parentName:"tr",align:null},"String"),(0,a.kt)("td",{parentName:"tr",align:null},"true"),(0,a.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("li",null,"Comma-separated zookeeper hosts with chroot used in the SolrCloud mode. ",(0,a.kt)("br",null),(0,a.kt)("strong",{parentName:"td"},"Example"),(0,a.kt)("br",null),(0,a.kt)("inlineCode",{parentName:"td"},"localhost:2181,localhost:2182/chroot")," ",(0,a.kt)("br",null),(0,a.kt)("br",null)),(0,a.kt)("li",null,"URL to connect to Solr used in standalone mode. ",(0,a.kt)("br",null),(0,a.kt)("strong",{parentName:"td"},"Example"),(0,a.kt)("br",null),(0,a.kt)("inlineCode",{parentName:"td"},"localhost:8983/solr")," "))),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"solrMode")),(0,a.kt)("td",{parentName:"tr",align:null},"String"),(0,a.kt)("td",{parentName:"tr",align:null},"true"),(0,a.kt)("td",{parentName:"tr",align:null},"SolrCloud"),(0,a.kt)("td",{parentName:"tr",align:null},"The client mode when interacting with the Solr cluster. ",(0,a.kt)("br",null),(0,a.kt)("br",null),"Below are the available options:",(0,a.kt)("br",null),(0,a.kt)("li",null,"Standalone",(0,a.kt)("br",null)),(0,a.kt)("li",null," SolrCloud"))),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"solrCollection")),(0,a.kt)("td",{parentName:"tr",align:null},"String"),(0,a.kt)("td",{parentName:"tr",align:null},"true"),(0,a.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,a.kt)("td",{parentName:"tr",align:null},"Solr collection name to which records need to be written.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"solrCommitWithinMs")),(0,a.kt)("td",{parentName:"tr",align:null},"int"),(0,a.kt)("td",{parentName:"tr",align:null},"false"),(0,a.kt)("td",{parentName:"tr",align:null},"10"),(0,a.kt)("td",{parentName:"tr",align:null},"The time within million seconds for Solr updating commits.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"username")),(0,a.kt)("td",{parentName:"tr",align:null},"String"),(0,a.kt)("td",{parentName:"tr",align:null},"false"),(0,a.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,a.kt)("td",{parentName:"tr",align:null},"The username for basic authentication.",(0,a.kt)("br",null),(0,a.kt)("br",null),(0,a.kt)("strong",{parentName:"td"},"Note: ",(0,a.kt)("inlineCode",{parentName:"strong"},"usename")," is case-sensitive."))),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"password")),(0,a.kt)("td",{parentName:"tr",align:null},"String"),(0,a.kt)("td",{parentName:"tr",align:null},"false"),(0,a.kt)("td",{parentName:"tr",align:null},'" " (empty string)'),(0,a.kt)("td",{parentName:"tr",align:null},"The password for basic authentication. ",(0,a.kt)("br",null),(0,a.kt)("br",null),(0,a.kt)("strong",{parentName:"td"},"Note: ",(0,a.kt)("inlineCode",{parentName:"strong"},"password")," is case-sensitive."))))),(0,a.kt)("h3",{id:"example"},"Example"),(0,a.kt)("p",null,"Before using the Solr sink connector, you need to create a configuration file through one of the following methods."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"JSON"),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n    "solrUrl": "localhost:2181,localhost:2182/chroot",\n    "solrMode": "SolrCloud",\n    "solrCollection": "techproducts",\n    "solrCommitWithinMs": 100,\n    "username": "fakeuser",\n    "password": "fake@123"\n}\n\n'))),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"YAML"),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},'\n{\n    solrUrl: "localhost:2181,localhost:2182/chroot"\n    solrMode: "SolrCloud"\n    solrCollection: "techproducts"\n    solrCommitWithinMs: 100\n    username: "fakeuser"\n    password: "fake@123"\n}\n\n')))))}m.isMDXComponent=!0},86010:function(t,e,n){function r(t){var e,n,l="";if("string"==typeof t||"number"==typeof t)l+=t;else if("object"==typeof t)if(Array.isArray(t))for(e=0;e<t.length;e++)t[e]&&(n=r(t[e]))&&(l&&(l+=" "),l+=n);else for(e in t)t[e]&&(l&&(l+=" "),l+=e);return l}function l(){for(var t,e,n=0,l="";n<arguments.length;)(t=arguments[n++])&&(e=r(t))&&(l&&(l+=" "),l+=e);return l}n.d(e,{Z:function(){return l}})}}]);
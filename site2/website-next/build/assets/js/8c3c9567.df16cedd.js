"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[50638],{3905:function(e,t,r){r.d(t,{Zo:function(){return s},kt:function(){return d}});var n=r(67294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function a(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function c(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var u=n.createContext({}),l=function(e){var t=n.useContext(u),r=t;return e&&(r="function"==typeof e?e(t):a(a({},t),e)),r},s=function(e){var t=l(e.components);return n.createElement(u.Provider,{value:t},e.children)},f={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},p=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,i=e.originalType,u=e.parentName,s=c(e,["components","mdxType","originalType","parentName"]),p=l(r),d=o,m=p["".concat(u,".").concat(d)]||p[d]||f[d]||i;return r?n.createElement(m,a(a({ref:t},s),{},{components:r})):n.createElement(m,a({ref:t},s))}));function d(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var i=r.length,a=new Array(i);a[0]=p;var c={};for(var u in t)hasOwnProperty.call(t,u)&&(c[u]=t[u]);c.originalType=e,c.mdxType="string"==typeof e?e:o,a[1]=c;for(var l=2;l<i;l++)a[l]=r[l];return n.createElement.apply(null,a)}return n.createElement.apply(null,r)}p.displayName="MDXCreateElement"},58215:function(e,t,r){var n=r(67294);t.Z=function(e){var t=e.children,r=e.hidden,o=e.className;return n.createElement("div",{role:"tabpanel",hidden:r,className:o},t)}},55064:function(e,t,r){r.d(t,{Z:function(){return l}});var n=r(67294),o=r(79443);var i=function(){var e=(0,n.useContext)(o.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},a=r(86010),c="tabItem_1uMI",u="tabItemActive_2DSg";var l=function(e){var t,r=e.lazy,o=e.block,l=e.defaultValue,s=e.values,f=e.groupId,p=e.className,d=n.Children.toArray(e.children),m=null!=s?s:d.map((function(e){return{value:e.props.value,label:e.props.label}})),v=null!=l?l:null==(t=d.find((function(e){return e.props.default})))?void 0:t.props.value,b=i(),y=b.tabGroupChoices,w=b.setTabGroupChoices,h=(0,n.useState)(v),g=h[0],O=h[1],C=[];if(null!=f){var x=y[f];null!=x&&x!==g&&m.some((function(e){return e.value===x}))&&O(x)}var E=function(e){var t=e.currentTarget,r=C.indexOf(t),n=m[r].value;O(n),null!=f&&(w(f,n),setTimeout((function(){var e,r,n,o,i,a,c,l;(e=t.getBoundingClientRect(),r=e.top,n=e.left,o=e.bottom,i=e.right,a=window,c=a.innerHeight,l=a.innerWidth,r>=0&&i<=l&&o<=c&&n>=0)||(t.scrollIntoView({block:"center",behavior:"smooth"}),t.classList.add(u),setTimeout((function(){return t.classList.remove(u)}),2e3))}),150))},T=function(e){var t,r=null;switch(e.key){case"ArrowRight":var n=C.indexOf(e.target)+1;r=C[n]||C[0];break;case"ArrowLeft":var o=C.indexOf(e.target)-1;r=C[o]||C[C.length-1]}null==(t=r)||t.focus()};return n.createElement("div",{className:"tabs-container"},n.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,a.Z)("tabs",{"tabs--block":o},p)},m.map((function(e){var t=e.value,r=e.label;return n.createElement("li",{role:"tab",tabIndex:g===t?0:-1,"aria-selected":g===t,className:(0,a.Z)("tabs__item",c,{"tabs__item--active":g===t}),key:t,ref:function(e){return C.push(e)},onKeyDown:T,onFocus:E,onClick:E},null!=r?r:t)}))),r?(0,n.cloneElement)(d.filter((function(e){return e.props.value===g}))[0],{className:"margin-vert--md"}):n.createElement("div",{className:"margin-vert--md"},d.map((function(e,t){return(0,n.cloneElement)(e,{key:t,hidden:e.props.value!==g})}))))}},79443:function(e,t,r){var n=(0,r(67294).createContext)(void 0);t.Z=n},19194:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return c},contentTitle:function(){return u},metadata:function(){return l},toc:function(){return s},default:function(){return p}});var n=r(87462),o=r(63366),i=(r(67294),r(3905)),a=(r(55064),r(58215),["components"]),c={id:"io-twitter",title:"Twitter Firehose Connector",sidebar_label:"Twitter Firehose Connector",original_id:"io-twitter"},u=void 0,l={unversionedId:"io-twitter",id:"version-2.7.0/io-twitter",isDocsHomePage:!1,title:"Twitter Firehose Connector",description:"",source:"@site/versioned_docs/version-2.7.0/io-twitter.md",sourceDirName:".",slug:"/io-twitter",permalink:"/docs/2.7.0/io-twitter",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.0/io-twitter.md",tags:[],version:"2.7.0",frontMatter:{id:"io-twitter",title:"Twitter Firehose Connector",sidebar_label:"Twitter Firehose Connector",original_id:"io-twitter"}},s=[],f={toc:s};function p(e){var t=e.components,r=(0,o.Z)(e,a);return(0,i.kt)("wrapper",(0,n.Z)({},f,r,{components:t,mdxType:"MDXLayout"}))}p.isMDXComponent=!0},86010:function(e,t,r){function n(e){var t,r,o="";if("string"==typeof e||"number"==typeof e)o+=e;else if("object"==typeof e)if(Array.isArray(e))for(t=0;t<e.length;t++)e[t]&&(r=n(e[t]))&&(o&&(o+=" "),o+=r);else for(t in e)e[t]&&(o&&(o+=" "),o+=t);return o}function o(){for(var e,t,r=0,o="";r<arguments.length;)(e=arguments[r++])&&(t=n(e))&&(o&&(o+=" "),o+=t);return o}r.d(t,{Z:function(){return o}})}}]);
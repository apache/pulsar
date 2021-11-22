"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[26506],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return d}});var r=n(67294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),u=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},p=function(e){var t=u(e.components);return r.createElement(s.Provider,{value:t},e.children)},l={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},h=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,i=e.originalType,s=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),h=u(n),d=o,f=h["".concat(s,".").concat(d)]||h[d]||l[d]||i;return n?r.createElement(f,a(a({ref:t},p),{},{components:n})):r.createElement(f,a({ref:t},p))}));function d(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var i=n.length,a=new Array(i);a[0]=h;var c={};for(var s in t)hasOwnProperty.call(t,s)&&(c[s]=t[s]);c.originalType=e,c.mdxType="string"==typeof e?e:o,a[1]=c;for(var u=2;u<i;u++)a[u]=n[u];return r.createElement.apply(null,a)}return r.createElement.apply(null,n)}h.displayName="MDXCreateElement"},21095:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return c},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return p},default:function(){return h}});var r=n(87462),o=n(63366),i=(n(67294),n(3905)),a=["components"],c={id:"concepts-authentication",title:"Authentication and Authorization",sidebar_label:"Authentication and Authorization",original_id:"concepts-authentication"},s=void 0,u={unversionedId:"concepts-authentication",id:"version-2.7.3/concepts-authentication",isDocsHomePage:!1,title:"Authentication and Authorization",description:"Pulsar supports a pluggable authentication mechanism which can be configured at the proxy and/or the broker. Pulsar also supports a pluggable authorization mechanism. These mechanisms work together to identify the client and its access rights on topics, namespaces and tenants.",source:"@site/versioned_docs/version-2.7.3/concepts-authentication.md",sourceDirName:".",slug:"/concepts-authentication",permalink:"/docs/2.7.3/concepts-authentication",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.3/concepts-authentication.md",tags:[],version:"2.7.3",frontMatter:{id:"concepts-authentication",title:"Authentication and Authorization",sidebar_label:"Authentication and Authorization",original_id:"concepts-authentication"},sidebar:"version-2.7.3/docsSidebar",previous:{title:"Multi Tenancy",permalink:"/docs/2.7.3/concepts-multi-tenancy"},next:{title:"Topic Compaction",permalink:"/docs/2.7.3/concepts-topic-compaction"}},p=[],l={toc:p};function h(e){var t=e.components,n=(0,o.Z)(e,a);return(0,i.kt)("wrapper",(0,r.Z)({},l,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Pulsar supports a pluggable ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.7.3/security-overview"},"authentication")," mechanism which can be configured at the proxy and/or the broker. Pulsar also supports a pluggable ",(0,i.kt)("a",{parentName:"p",href:"security-authorization"},"authorization")," mechanism. These mechanisms work together to identify the client and its access rights on topics, namespaces and tenants."))}h.isMDXComponent=!0}}]);
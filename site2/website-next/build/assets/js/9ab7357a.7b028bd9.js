"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[34230],{3905:function(e,t,r){r.d(t,{Zo:function(){return p},kt:function(){return g}});var o=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,o)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function c(e,t){if(null==e)return{};var r,o,n=function(e,t){if(null==e)return{};var r,o,n={},a=Object.keys(e);for(o=0;o<a.length;o++)r=a[o],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(o=0;o<a.length;o++)r=a[o],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var s=o.createContext({}),l=function(e){var t=o.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=l(e.components);return o.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},d=o.forwardRef((function(e,t){var r=e.components,n=e.mdxType,a=e.originalType,s=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),d=l(r),g=n,f=d["".concat(s,".").concat(g)]||d[g]||u[g]||a;return r?o.createElement(f,i(i({ref:t},p),{},{components:r})):o.createElement(f,i({ref:t},p))}));function g(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var a=r.length,i=new Array(a);i[0]=d;var c={};for(var s in t)hasOwnProperty.call(t,s)&&(c[s]=t[s]);c.originalType=e,c.mdxType="string"==typeof e?e:n,i[1]=c;for(var l=2;l<a;l++)i[l]=r[l];return o.createElement.apply(null,i)}return o.createElement.apply(null,r)}d.displayName="MDXCreateElement"},32964:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return c},contentTitle:function(){return s},metadata:function(){return l},toc:function(){return p},default:function(){return d}});var o=r(87462),n=r(63366),a=(r(67294),r(3905)),i=["components"],c={id:"concepts-tiered-storage",title:"Tiered Storage",sidebar_label:"Tiered Storage",original_id:"concepts-tiered-storage"},s=void 0,l={unversionedId:"concepts-tiered-storage",id:"version-2.6.4/concepts-tiered-storage",isDocsHomePage:!1,title:"Tiered Storage",description:"Pulsar's segment oriented architecture allows for topic backlogs to grow very large, effectively without limit. However, this can become expensive over time.",source:"@site/versioned_docs/version-2.6.4/concepts-tiered-storage.md",sourceDirName:".",slug:"/concepts-tiered-storage",permalink:"/docs/2.6.4/concepts-tiered-storage",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.4/concepts-tiered-storage.md",tags:[],version:"2.6.4",frontMatter:{id:"concepts-tiered-storage",title:"Tiered Storage",sidebar_label:"Tiered Storage",original_id:"concepts-tiered-storage"},sidebar:"version-2.6.4/docsSidebar",previous:{title:"Topic Compaction",permalink:"/docs/2.6.4/concepts-topic-compaction"},next:{title:"Proxy support with SNI routing",permalink:"/docs/2.6.4/concepts-proxy-sni-routing"}},p=[],u={toc:p};function d(e){var t=e.components,c=(0,n.Z)(e,i);return(0,a.kt)("wrapper",(0,o.Z)({},u,c,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"Pulsar's segment oriented architecture allows for topic backlogs to grow very large, effectively without limit. However, this can become expensive over time."),(0,a.kt)("p",null,"One way to alleviate this cost is to use Tiered Storage. With tiered storage, older messages in the backlog can be moved from BookKeeper to a cheaper storage mechanism, while still allowing clients to access the backlog as if nothing had changed."),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"Tiered Storage",src:r(62006).Z})),(0,a.kt)("blockquote",null,(0,a.kt)("p",{parentName:"blockquote"},"Data written to BookKeeper is replicated to 3 physical machines by default. However, once a segment is sealed in BookKeeper it becomes immutable and can be copied to long term storage. Long term storage can achieve cost savings by using mechanisms such as ",(0,a.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction"},"Reed-Solomon error correction")," to require fewer physical copies of data.")),(0,a.kt)("p",null,"Pulsar currently supports S3, Google Cloud Storage (GCS), and filesystem for ",(0,a.kt)("a",{parentName:"p",href:"https://pulsar.apache.org/docs/en/cookbooks-tiered-storage/"},"long term store"),". Offloading to long term storage triggered via a Rest API or command line interface. The user passes in the amount of topic data they wish to retain on BookKeeper, and the broker will copy the backlog data to long term storage. The original data will then be deleted from BookKeeper after a configured delay (4 hours by default)."),(0,a.kt)("blockquote",null,(0,a.kt)("p",{parentName:"blockquote"},"For a guide for setting up tiered storage, see the ",(0,a.kt)("a",{parentName:"p",href:"cookbooks-tiered-storage"},"Tiered storage cookbook"),".")))}d.isMDXComponent=!0},62006:function(e,t,r){t.Z=r.p+"assets/images/pulsar-tiered-storage-72d8b53762992cfeaa58ae3b48dd2522.png"}}]);
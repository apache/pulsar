"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[21655],{3905:function(e,t,r){r.d(t,{Zo:function(){return u},kt:function(){return h}});var a=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var c=a.createContext({}),s=function(e){var t=a.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=s(e.components);return a.createElement(c.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},p=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,o=e.originalType,c=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),p=s(r),h=n,d=p["".concat(c,".").concat(h)]||p[h]||m[h]||o;return r?a.createElement(d,i(i({ref:t},u),{},{components:r})):a.createElement(d,i({ref:t},u))}));function h(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=r.length,i=new Array(o);i[0]=p;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l.mdxType="string"==typeof e?e:n,i[1]=l;for(var s=2;s<o;s++)i[s]=r[s];return a.createElement.apply(null,i)}return a.createElement.apply(null,r)}p.displayName="MDXCreateElement"},58215:function(e,t,r){var a=r(67294);t.Z=function(e){var t=e.children,r=e.hidden,n=e.className;return a.createElement("div",{role:"tabpanel",hidden:r,className:n},t)}},55064:function(e,t,r){r.d(t,{Z:function(){return s}});var a=r(67294),n=r(79443);var o=function(){var e=(0,a.useContext)(n.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},i=r(86010),l="tabItem_1uMI",c="tabItemActive_2DSg";var s=function(e){var t,r=e.lazy,n=e.block,s=e.defaultValue,u=e.values,m=e.groupId,p=e.className,h=a.Children.toArray(e.children),d=null!=u?u:h.map((function(e){return{value:e.props.value,label:e.props.label}})),f=null!=s?s:null==(t=h.find((function(e){return e.props.default})))?void 0:t.props.value,g=o(),v=g.tabGroupChoices,y=g.setTabGroupChoices,b=(0,a.useState)(f),k=b[0],S=b[1],w=[];if(null!=m){var N=v[m];null!=N&&N!==k&&d.some((function(e){return e.value===N}))&&S(N)}var C=function(e){var t=e.currentTarget,r=w.indexOf(t),a=d[r].value;S(a),null!=m&&(y(m,a),setTimeout((function(){var e,r,a,n,o,i,l,s;(e=t.getBoundingClientRect(),r=e.top,a=e.left,n=e.bottom,o=e.right,i=window,l=i.innerHeight,s=i.innerWidth,r>=0&&o<=s&&n<=l&&a>=0)||(t.scrollIntoView({block:"center",behavior:"smooth"}),t.classList.add(c),setTimeout((function(){return t.classList.remove(c)}),2e3))}),150))},x=function(e){var t,r=null;switch(e.key){case"ArrowRight":var a=w.indexOf(e.target)+1;r=w[a]||w[0];break;case"ArrowLeft":var n=w.indexOf(e.target)-1;r=w[n]||w[w.length-1]}null==(t=r)||t.focus()};return a.createElement("div",{className:"tabs-container"},a.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":n},p)},d.map((function(e){var t=e.value,r=e.label;return a.createElement("li",{role:"tab",tabIndex:k===t?0:-1,"aria-selected":k===t,className:(0,i.Z)("tabs__item",l,{"tabs__item--active":k===t}),key:t,ref:function(e){return w.push(e)},onKeyDown:x,onFocus:C,onClick:C},null!=r?r:t)}))),r?(0,a.cloneElement)(h.filter((function(e){return e.props.value===k}))[0],{className:"margin-vert--md"}):a.createElement("div",{className:"margin-vert--md"},h.map((function(e,t){return(0,a.cloneElement)(e,{key:t,hidden:e.props.value!==k})}))))}},79443:function(e,t,r){var a=(0,r(67294).createContext)(void 0);t.Z=a},24726:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return l},contentTitle:function(){return c},metadata:function(){return s},toc:function(){return u},default:function(){return p}});var a=r(87462),n=r(63366),o=(r(67294),r(3905)),i=(r(55064),r(58215),["components"]),l={id:"develop-schema",title:"Custom schema storage",sidebar_label:"Custom schema storage",original_id:"develop-schema"},c=void 0,s={unversionedId:"develop-schema",id:"develop-schema",isDocsHomePage:!1,title:"Custom schema storage",description:"By default, Pulsar stores data type schemas in Apache BookKeeper (which is deployed alongside Pulsar). You can, however, use another storage system if you wish. This doc walks you through creating your own schema storage implementation.",source:"@site/docs/develop-schema.md",sourceDirName:".",slug:"/develop-schema",permalink:"/docs/next/develop-schema",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/docs/develop-schema.md",tags:[],version:"current",frontMatter:{id:"develop-schema",title:"Custom schema storage",sidebar_label:"Custom schema storage",original_id:"develop-schema"},sidebar:"docsSidebar",previous:{title:"Binary protocol",permalink:"/docs/next/develop-binary-protocol"},next:{title:"Modular load manager",permalink:"/docs/next/develop-load-manager"}},u=[{value:"SchemaStorage interface",id:"schemastorage-interface",children:[]},{value:"SchemaStorageFactory interface",id:"schemastoragefactory-interface",children:[]},{value:"Deployment",id:"deployment",children:[]}],m={toc:u};function p(e){var t=e.components,r=(0,n.Z)(e,i);return(0,o.kt)("wrapper",(0,a.Z)({},m,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"By default, Pulsar stores data type ",(0,o.kt)("a",{parentName:"p",href:"concepts-schema-registry"},"schemas")," in ",(0,o.kt)("a",{parentName:"p",href:"https://bookkeeper.apache.org"},"Apache BookKeeper")," (which is deployed alongside Pulsar). You can, however, use another storage system if you wish. This doc walks you through creating your own schema storage implementation."),(0,o.kt)("p",null,"In order to use a non-default (i.e. non-BookKeeper) storage system for Pulsar schemas, you need to implement two Java interfaces: ",(0,o.kt)("a",{parentName:"p",href:"#schemastorage-interface"},(0,o.kt)("inlineCode",{parentName:"a"},"SchemaStorage"))," and ",(0,o.kt)("a",{parentName:"p",href:"#schemastoragefactory-interface"},(0,o.kt)("inlineCode",{parentName:"a"},"SchemaStorageFactory")),"."),(0,o.kt)("h2",{id:"schemastorage-interface"},"SchemaStorage interface"),(0,o.kt)("p",null,"The ",(0,o.kt)("inlineCode",{parentName:"p"},"SchemaStorage")," interface has the following methods:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"\npublic interface SchemaStorage {\n    // How schemas are updated\n    CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash);\n\n    // How schemas are fetched from storage\n    CompletableFuture<StoredSchema> get(String key, SchemaVersion version);\n\n    // How schemas are deleted\n    CompletableFuture<SchemaVersion> delete(String key);\n\n    // Utility method for converting a schema version byte array to a SchemaVersion object\n    SchemaVersion versionFromBytes(byte[] version);\n\n    // Startup behavior for the schema storage client\n    void start() throws Exception;\n\n    // Shutdown behavior for the schema storage client\n    void close() throws Exception;\n}\n\n")),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"For a full-fledged example schema storage implementation, see the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/BookkeeperSchemaStorage.java"},(0,o.kt)("inlineCode",{parentName:"a"},"BookKeeperSchemaStorage"))," class.")),(0,o.kt)("h2",{id:"schemastoragefactory-interface"},"SchemaStorageFactory interface"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},"\npublic interface SchemaStorageFactory {\n    @NotNull\n    SchemaStorage create(PulsarService pulsar) throws Exception;\n}\n\n")),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"For a full-fledged example schema storage factory implementation, see the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/BookkeeperSchemaStorageFactory.java"},(0,o.kt)("inlineCode",{parentName:"a"},"BookKeeperSchemaStorageFactory"))," class.")),(0,o.kt)("h2",{id:"deployment"},"Deployment"),(0,o.kt)("p",null,"In order to use your custom schema storage implementation, you'll need to:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Package the implementation in a ",(0,o.kt)("a",{parentName:"li",href:"https://docs.oracle.com/javase/tutorial/deployment/jar/basicsindex.html"},"JAR")," file."),(0,o.kt)("li",{parentName:"ol"},"Add that jar to the ",(0,o.kt)("inlineCode",{parentName:"li"},"lib")," folder in your Pulsar ",(0,o.kt)("a",{parentName:"li",href:"/docs/next/#installing-pulsar"},"binary or source distribution"),"."),(0,o.kt)("li",{parentName:"ol"},"Change the ",(0,o.kt)("inlineCode",{parentName:"li"},"schemaRegistryStorageClassName")," configuration in ",(0,o.kt)("a",{parentName:"li",href:"/docs/next/reference-configuration#broker"},(0,o.kt)("inlineCode",{parentName:"a"},"broker.conf"))," to your custom factory class (i.e. the ",(0,o.kt)("inlineCode",{parentName:"li"},"SchemaStorageFactory")," implementation, not the ",(0,o.kt)("inlineCode",{parentName:"li"},"SchemaStorage")," implementation)."),(0,o.kt)("li",{parentName:"ol"},"Start up Pulsar.")))}p.isMDXComponent=!0},86010:function(e,t,r){function a(e){var t,r,n="";if("string"==typeof e||"number"==typeof e)n+=e;else if("object"==typeof e)if(Array.isArray(e))for(t=0;t<e.length;t++)e[t]&&(r=a(e[t]))&&(n&&(n+=" "),n+=r);else for(t in e)e[t]&&(n&&(n+=" "),n+=t);return n}function n(){for(var e,t,r=0,n="";r<arguments.length;)(e=arguments[r++])&&(t=a(e))&&(n&&(n+=" "),n+=t);return n}r.d(t,{Z:function(){return n}})}}]);
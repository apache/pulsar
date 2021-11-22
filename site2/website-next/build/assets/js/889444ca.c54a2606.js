"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[56184],{3905:function(e,t,a){a.d(t,{Zo:function(){return p},kt:function(){return d}});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),u=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=u(e.components);return n.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),m=u(a),d=r,h=m["".concat(s,".").concat(d)]||m[d]||c[d]||i;return a?n.createElement(h,l(l({ref:t},p),{},{components:a})):n.createElement(h,l({ref:t},p))}));function d(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,l=new Array(i);l[0]=m;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:r,l[1]=o;for(var u=2;u<i;u++)l[u]=a[u];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},58288:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return o},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return p},default:function(){return m}});var n=a(87462),r=a(63366),i=(a(67294),a(3905)),l=["components"],o={id:"admin-api-overview",title:"The Pulsar admin interface",sidebar_label:"Overview",original_id:"admin-api-overview"},s=void 0,u={unversionedId:"admin-api-overview",id:"version-2.6.3/admin-api-overview",isDocsHomePage:!1,title:"The Pulsar admin interface",description:"The Pulsar admin interface enables you to manage all of the important entities in a Pulsar instance, such as tenants, topics, and namespaces.",source:"@site/versioned_docs/version-2.6.3/admin-api-overview.md",sourceDirName:".",slug:"/admin-api-overview",permalink:"/docs/2.6.3/admin-api-overview",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.3/admin-api-overview.md",tags:[],version:"2.6.3",frontMatter:{id:"admin-api-overview",title:"The Pulsar admin interface",sidebar_label:"Overview",original_id:"admin-api-overview"},sidebar:"version-2.6.3/docsSidebar",previous:{title:"C#",permalink:"/docs/2.6.3/client-libraries-dotnet"},next:{title:"Clusters",permalink:"/docs/2.6.3/admin-api-clusters"}},p=[{value:"Admin setup",id:"admin-setup",children:[{value:"pulsar-admin",id:"pulsar-admin",children:[]},{value:"REST API",id:"rest-api",children:[]},{value:"Java admin client",id:"java-admin-client",children:[]}]}],c={toc:p};function m(e){var t=e.components,a=(0,r.Z)(e,l);return(0,i.kt)("wrapper",(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"The Pulsar admin interface enables you to manage all of the important entities in a Pulsar ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-terminology#instance"},"instance"),", such as ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-terminology#tenant"},"tenants"),", ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-terminology#topic"},"topics"),", and ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-terminology#namespace"},"namespaces"),"."),(0,i.kt)("p",null,"You can currently interact with the admin interface via:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Making HTTP calls against the admin ",(0,i.kt)("a",{parentName:"li",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"REST")," API provided by Pulsar ",(0,i.kt)("a",{parentName:"li",href:"/docs/2.6.3/reference-terminology#broker"},"brokers"),". For some restful apis, they might be redirected to topic owner brokers for serving\nwith ",(0,i.kt)("a",{parentName:"li",href:"https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/307"},(0,i.kt)("inlineCode",{parentName:"a"},"307 Temporary Redirect")),", hence the HTTP callers should handle ",(0,i.kt)("inlineCode",{parentName:"li"},"307 Temporary Redirect"),". If you are using ",(0,i.kt)("inlineCode",{parentName:"li"},"curl"),", you should specify ",(0,i.kt)("inlineCode",{parentName:"li"},"-L"),"\nto handle redirections."),(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("inlineCode",{parentName:"li"},"pulsar-admin")," CLI tool, which is available in the ",(0,i.kt)("inlineCode",{parentName:"li"},"bin")," folder of your ",(0,i.kt)("a",{parentName:"li",href:"getting-started-standalone"},"Pulsar installation"),":")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ bin/pulsar-admin\n\n")),(0,i.kt)("p",null,"Full documentation for this tool can be found in the ",(0,i.kt)("a",{parentName:"p",href:"reference-pulsar-admin"},"Pulsar command-line tools")," doc."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"A Java client interface.")),(0,i.kt)("blockquote",null,(0,i.kt)("h4",{parentName:"blockquote",id:"the-rest-api-is-the-admin-interface"},"The REST API is the admin interface"),(0,i.kt)("p",{parentName:"blockquote"},"Under the hood, both the ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar-admin")," CLI tool and the Java client both use the REST API. If you\u2019d like to implement your own admin interface client, you should use the REST API as well. Full documentation can be found here.")),(0,i.kt)("p",null,"In this document, examples from each of the three available interfaces will be shown."),(0,i.kt)("h2",{id:"admin-setup"},"Admin setup"),(0,i.kt)("p",null,"Each of Pulsar's three admin interfaces---the ",(0,i.kt)("a",{parentName:"p",href:"reference-pulsar-admin"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar-admin"))," CLI tool, the ",(0,i.kt)("a",{parentName:"p",href:"/api/admin/2.6.0-SNAPSHOT"},"Java admin API"),", and the ",(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"REST")," API ---requires some special setup if you have ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/security-overview#authentication-providers"},"authentication")," enabled in your Pulsar ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-terminology#instance"},"instance"),"."),(0,i.kt)("h3",{id:"pulsar-admin"},"pulsar-admin"),(0,i.kt)("p",null,"If you have ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/security-overview#authentication-providers"},"authentication")," enabled, you will need to provide an auth configuration to use the ",(0,i.kt)("a",{parentName:"p",href:"reference-pulsar-admin"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar-admin"))," tool. By default, the configuration for the ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar-admin")," tool is found in the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-configuration#client"},(0,i.kt)("inlineCode",{parentName:"a"},"conf/client.conf"))," file. Here are the available parameters:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Name"),(0,i.kt)("th",{parentName:"tr",align:null},"Description"),(0,i.kt)("th",{parentName:"tr",align:null},"Default"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"webServiceUrl"),(0,i.kt)("td",{parentName:"tr",align:null},"The web URL for the cluster."),(0,i.kt)("td",{parentName:"tr",align:null},"http://localhost:8080/")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"brokerServiceUrl"),(0,i.kt)("td",{parentName:"tr",align:null},"The Pulsar protocol URL for the cluster."),(0,i.kt)("td",{parentName:"tr",align:null},"pulsar://localhost:6650/")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"authPlugin"),(0,i.kt)("td",{parentName:"tr",align:null},"The authentication plugin."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"authParams"),(0,i.kt)("td",{parentName:"tr",align:null},"The authentication parameters for the cluster, as a comma-separated string."),(0,i.kt)("td",{parentName:"tr",align:null})),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"useTls"),(0,i.kt)("td",{parentName:"tr",align:null},"Whether or not TLS authentication will be enforced in the cluster."),(0,i.kt)("td",{parentName:"tr",align:null},"false")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"tlsAllowInsecureConnection"),(0,i.kt)("td",{parentName:"tr",align:null},"Accept untrusted TLS certificate from client."),(0,i.kt)("td",{parentName:"tr",align:null},"false")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"tlsTrustCertsFilePath"),(0,i.kt)("td",{parentName:"tr",align:null},"Path for the trusted TLS certificate file."),(0,i.kt)("td",{parentName:"tr",align:null})))),(0,i.kt)("h3",{id:"rest-api"},"REST API"),(0,i.kt)("p",null,"You can find documentation for the REST API exposed by Pulsar ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-terminology#broker"},"brokers")," in this reference ",(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/admin-rest-api#/"},"document"),"."),(0,i.kt)("h3",{id:"java-admin-client"},"Java admin client"),(0,i.kt)("p",null,"To use the Java admin API, instantiate a ",(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/api/admin/org/apache/pulsar/client/admin/PulsarAdmin"},"PulsarAdmin")," object, specifying a URL for a Pulsar ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.3/reference-terminology#broker"},"broker")," and a ",(0,i.kt)("a",{parentName:"p",href:"https://pulsar.incubator.apache.org/api/admin/org/apache/pulsar/client/admin/PulsarAdminBuilder"},"PulsarAdminBuilder"),". Here's a minimal example using ",(0,i.kt)("inlineCode",{parentName:"p"},"localhost"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'\nString url = "http://localhost:8080";\n// Pass auth-plugin class fully-qualified name if Pulsar-security enabled\nString authPluginClassName = "com.org.MyAuthPluginClass";\n// Pass auth-param if auth-plugin class requires it\nString authParams = "param1=value1";\nboolean useTls = false;\nboolean tlsAllowInsecureConnection = false;\nString tlsTrustCertsFilePath = null;\nPulsarAdmin admin = PulsarAdmin.builder()\n.authentication(authPluginClassName,authParams)\n.serviceHttpUrl(url)\n.tlsTrustCertsFilePath(tlsTrustCertsFilePath)\n.allowTlsInsecureConnection(tlsAllowInsecureConnection)\n.build();\n\n')),(0,i.kt)("p",null,"If you have multiple brokers to use, you can use multi-host like Pulsar service. For example,"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'\nString url = "http://localhost:8080,localhost:8081,localhost:8082";\n// Pass auth-plugin class fully-qualified name if Pulsar-security enabled\nString authPluginClassName = "com.org.MyAuthPluginClass";\n// Pass auth-param if auth-plugin class requires it\nString authParams = "param1=value1";\nboolean useTls = false;\nboolean tlsAllowInsecureConnection = false;\nString tlsTrustCertsFilePath = null;\nPulsarAdmin admin = PulsarAdmin.builder()\n.authentication(authPluginClassName,authParams)\n.serviceHttpUrl(url)\n.tlsTrustCertsFilePath(tlsTrustCertsFilePath)\n.allowTlsInsecureConnection(tlsAllowInsecureConnection)\n.build();\n\n')))}m.isMDXComponent=!0}}]);
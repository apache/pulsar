"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[34048],{3905:function(e,n,t){t.d(n,{Zo:function(){return u},kt:function(){return m}});var a=t(67294);function r(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function i(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);n&&(a=a.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,a)}return t}function l(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?i(Object(t),!0).forEach((function(n){r(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):i(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function s(e,n){if(null==e)return{};var t,a,r=function(e,n){if(null==e)return{};var t,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)t=i[a],n.indexOf(t)>=0||(r[t]=e[t]);return r}(e,n);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)t=i[a],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var o=a.createContext({}),p=function(e){var n=a.useContext(o),t=n;return e&&(t="function"==typeof e?e(n):l(l({},n),e)),t},u=function(e){var n=p(e.components);return a.createElement(o.Provider,{value:n},e.children)},c={inlineCode:"code",wrapper:function(e){var n=e.children;return a.createElement(a.Fragment,{},n)}},d=a.forwardRef((function(e,n){var t=e.components,r=e.mdxType,i=e.originalType,o=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),d=p(t),m=r,f=d["".concat(o,".").concat(m)]||d[m]||c[m]||i;return t?a.createElement(f,l(l({ref:n},u),{},{components:t})):a.createElement(f,l({ref:n},u))}));function m(e,n){var t=arguments,r=n&&n.mdxType;if("string"==typeof e||r){var i=t.length,l=new Array(i);l[0]=d;var s={};for(var o in n)hasOwnProperty.call(n,o)&&(s[o]=n[o]);s.originalType=e,s.mdxType="string"==typeof e?e:r,l[1]=s;for(var p=2;p<i;p++)l[p]=t[p];return a.createElement.apply(null,l)}return a.createElement.apply(null,t)}d.displayName="MDXCreateElement"},19631:function(e,n,t){t.r(n),t.d(n,{frontMatter:function(){return s},contentTitle:function(){return o},metadata:function(){return p},toc:function(){return u},default:function(){return d}});var a=t(87462),r=t(63366),i=(t(67294),t(3905)),l=["components"],s={id:"admin-api-tenants",title:"Managing Tenants",sidebar_label:"Tenants",original_id:"admin-api-tenants"},o=void 0,p={unversionedId:"admin-api-tenants",id:"version-2.6.4/admin-api-tenants",isDocsHomePage:!1,title:"Managing Tenants",description:"Tenants, like namespaces, can be managed using the admin API. There are currently two configurable aspects of tenants:",source:"@site/versioned_docs/version-2.6.4/admin-api-tenants.md",sourceDirName:".",slug:"/admin-api-tenants",permalink:"/docs/2.6.4/admin-api-tenants",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.4/admin-api-tenants.md",tags:[],version:"2.6.4",frontMatter:{id:"admin-api-tenants",title:"Managing Tenants",sidebar_label:"Tenants",original_id:"admin-api-tenants"},sidebar:"version-2.6.4/docsSidebar",previous:{title:"Clusters",permalink:"/docs/2.6.4/admin-api-clusters"},next:{title:"Brokers",permalink:"/docs/2.6.4/admin-api-brokers"}},u=[{value:"Tenant resources",id:"tenant-resources",children:[{value:"List",id:"list",children:[]},{value:"Create",id:"create",children:[]},{value:"Get configuration",id:"get-configuration",children:[]},{value:"Delete",id:"delete",children:[]},{value:"Updating",id:"updating",children:[]}]}],c={toc:u};function d(e){var n=e.components,t=(0,r.Z)(e,l);return(0,i.kt)("wrapper",(0,a.Z)({},c,t,{components:n,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Tenants, like namespaces, can be managed using the ",(0,i.kt)("a",{parentName:"p",href:"admin-api-overview"},"admin API"),". There are currently two configurable aspects of tenants:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Admin roles"),(0,i.kt)("li",{parentName:"ul"},"Allowed clusters")),(0,i.kt)("h2",{id:"tenant-resources"},"Tenant resources"),(0,i.kt)("h3",{id:"list"},"List"),(0,i.kt)("h4",{id:"pulsar-admin"},"pulsar-admin"),(0,i.kt)("p",null,"You can list all of the tenants associated with an ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.4/reference-terminology#instance"},"instance")," using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.4/pulsar-admin#tenants-list"},(0,i.kt)("inlineCode",{parentName:"a"},"list"))," subcommand:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin tenants list\n\n")),(0,i.kt)("p",null,"That will return a simple list, like this:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\nmy-tenant-1\nmy-tenant-2\n\n")),(0,i.kt)("h3",{id:"create"},"Create"),(0,i.kt)("h4",{id:"pulsar-admin-1"},"pulsar-admin"),(0,i.kt)("p",null,"You can create a new tenant using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.4/pulsar-admin#tenants-create"},(0,i.kt)("inlineCode",{parentName:"a"},"create"))," subcommand:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin tenants create my-tenant\n\n")),(0,i.kt)("p",null,"When creating a tenant, you can assign admin roles using the ",(0,i.kt)("inlineCode",{parentName:"p"},"-r"),"/",(0,i.kt)("inlineCode",{parentName:"p"},"--admin-roles")," flag. You can specify multiple roles as a comma-separated list. Here are some examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin tenants create my-tenant \\\n  --admin-roles role1,role2,role3\n\n$ pulsar-admin tenants create my-tenant \\\n  -r role1\n\n")),(0,i.kt)("h3",{id:"get-configuration"},"Get configuration"),(0,i.kt)("h4",{id:"pulsar-admin-2"},"pulsar-admin"),(0,i.kt)("p",null,"You can see a tenant's configuration as a JSON object using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.4/pulsar-admin#tenants-get"},(0,i.kt)("inlineCode",{parentName:"a"},"get"))," subcommand and specifying the name of the tenant:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},'\n$ pulsar-admin tenants get my-tenant\n{\n  "adminRoles": [\n    "admin1",\n    "admin2"\n  ],\n  "allowedClusters": [\n    "cl1",\n    "cl2"\n  ]\n}\n\n')),(0,i.kt)("h3",{id:"delete"},"Delete"),(0,i.kt)("h4",{id:"pulsar-admin-3"},"pulsar-admin"),(0,i.kt)("p",null,"You can delete a tenant using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.4/pulsar-admin#tenants-delete"},(0,i.kt)("inlineCode",{parentName:"a"},"delete"))," subcommand and specifying the tenant name:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ pulsar-admin tenants delete my-tenant\n\n")),(0,i.kt)("h3",{id:"updating"},"Updating"),(0,i.kt)("h4",{id:"pulsar-admin-4"},"pulsar-admin"),(0,i.kt)("p",null,"You can update a tenant's configuration using the ",(0,i.kt)("a",{parentName:"p",href:"/docs/2.6.4/pulsar-admin#tenants-update"},(0,i.kt)("inlineCode",{parentName:"a"},"update"))," subcommand"))}d.isMDXComponent=!0}}]);
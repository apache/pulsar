"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[8490],{3905:function(e,t,n){n.d(t,{Zo:function(){return c},kt:function(){return d}});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=r.createContext({}),u=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=u(e.components);return r.createElement(l.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},h=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),h=u(n),d=a,m=h["".concat(l,".").concat(d)]||h[d]||p[d]||o;return n?r.createElement(m,i(i({ref:t},c),{},{components:n})):r.createElement(m,i({ref:t},c))}));function d(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=h;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:a,i[1]=s;for(var u=2;u<o;u++)i[u]=n[u];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}h.displayName="MDXCreateElement"},58215:function(e,t,n){var r=n(67294);t.Z=function(e){var t=e.children,n=e.hidden,a=e.className;return r.createElement("div",{role:"tabpanel",hidden:n,className:a},t)}},55064:function(e,t,n){n.d(t,{Z:function(){return u}});var r=n(67294),a=n(79443);var o=function(){var e=(0,r.useContext)(a.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},i=n(86010),s="tabItem_1uMI",l="tabItemActive_2DSg";var u=function(e){var t,n=e.lazy,a=e.block,u=e.defaultValue,c=e.values,p=e.groupId,h=e.className,d=r.Children.toArray(e.children),m=null!=c?c:d.map((function(e){return{value:e.props.value,label:e.props.label}})),f=null!=u?u:null==(t=d.find((function(e){return e.props.default})))?void 0:t.props.value,y=o(),k=y.tabGroupChoices,b=y.setTabGroupChoices,g=(0,r.useState)(f),v=g[0],w=g[1],N=[];if(null!=p){var x=k[p];null!=x&&x!==v&&m.some((function(e){return e.value===x}))&&w(x)}var P=function(e){var t=e.currentTarget,n=N.indexOf(t),r=m[n].value;w(r),null!=p&&(b(p,r),setTimeout((function(){var e,n,r,a,o,i,s,u;(e=t.getBoundingClientRect(),n=e.top,r=e.left,a=e.bottom,o=e.right,i=window,s=i.innerHeight,u=i.innerWidth,n>=0&&o<=u&&a<=s&&r>=0)||(t.scrollIntoView({block:"center",behavior:"smooth"}),t.classList.add(l),setTimeout((function(){return t.classList.remove(l)}),2e3))}),150))},C=function(e){var t,n=null;switch(e.key){case"ArrowRight":var r=N.indexOf(e.target)+1;n=N[r]||N[0];break;case"ArrowLeft":var a=N.indexOf(e.target)-1;n=N[a]||N[N.length-1]}null==(t=n)||t.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":a},h)},m.map((function(e){var t=e.value,n=e.label;return r.createElement("li",{role:"tab",tabIndex:v===t?0:-1,"aria-selected":v===t,className:(0,i.Z)("tabs__item",s,{"tabs__item--active":v===t}),key:t,ref:function(e){return N.push(e)},onKeyDown:C,onFocus:P,onClick:P},null!=n?n:t)}))),n?(0,r.cloneElement)(d.filter((function(e){return e.props.value===v}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},d.map((function(e,t){return(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==v})}))))}},79443:function(e,t,n){var r=(0,n(67294).createContext)(void 0);t.Z=r},28723:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return u},toc:function(){return c},default:function(){return h}});var r=n(87462),a=n(63366),o=(n(67294),n(3905)),i=(n(55064),n(58215),["components"]),s={id:"security-authorization",title:"Authentication and authorization in Pulsar",sidebar_label:"Authorization and ACLs",original_id:"security-authorization"},l=void 0,u={unversionedId:"security-authorization",id:"version-2.7.0/security-authorization",isDocsHomePage:!1,title:"Authentication and authorization in Pulsar",description:"In Pulsar, the authentication provider is responsible for properly identifying clients and associating the clients with role tokens. If you only enable authentication, an authenticated role token has the ability to access all resources in the cluster. Authorization is the process that determines what clients are able to do.",source:"@site/versioned_docs/version-2.7.0/security-authorization.md",sourceDirName:".",slug:"/security-authorization",permalink:"/docs/2.7.0/security-authorization",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.0/security-authorization.md",tags:[],version:"2.7.0",frontMatter:{id:"security-authorization",title:"Authentication and authorization in Pulsar",sidebar_label:"Authorization and ACLs",original_id:"security-authorization"},sidebar:"version-2.7.0/docsSidebar",previous:{title:"Authentication using OAuth 2.0 access tokens",permalink:"/docs/2.7.0/security-oauth2"},next:{title:"End-to-End Encryption",permalink:"/docs/2.7.0/security-encryption"}},c=[{value:"Broker and Proxy Setup",id:"broker-and-proxy-setup",children:[{value:"Enable authorization and assign superusers",id:"enable-authorization-and-assign-superusers",children:[]},{value:"Proxy Roles",id:"proxy-roles",children:[]}]},{value:"Administer tenants",id:"administer-tenants",children:[{value:"Create a new tenant",id:"create-a-new-tenant",children:[]},{value:"Manage permissions",id:"manage-permissions",children:[]},{value:"Pulsar admin authentication",id:"pulsar-admin-authentication",children:[]}]}],p={toc:c};function h(e){var t=e.components,n=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"In Pulsar, the ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/security-overview#authentication-providers"},"authentication provider")," is responsible for properly identifying clients and associating the clients with ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/security-overview#role-tokens"},"role tokens"),". If you only enable authentication, an authenticated role token has the ability to access all resources in the cluster. ",(0,o.kt)("em",{parentName:"p"},"Authorization")," is the process that determines ",(0,o.kt)("em",{parentName:"p"},"what")," clients are able to do."),(0,o.kt)("p",null,"The role tokens with the most privileges are the ",(0,o.kt)("em",{parentName:"p"},"superusers"),". The ",(0,o.kt)("em",{parentName:"p"},"superusers")," can create and destroy tenants, along with having full access to all tenant resources."),(0,o.kt)("p",null,"When a superuser creates a ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/reference-terminology#tenant"},"tenant"),", that tenant is assigned an admin role. A client with the admin role token can then create, modify and destroy namespaces, and grant and revoke permissions to ",(0,o.kt)("em",{parentName:"p"},"other role tokens")," on those namespaces."),(0,o.kt)("h2",{id:"broker-and-proxy-setup"},"Broker and Proxy Setup"),(0,o.kt)("h3",{id:"enable-authorization-and-assign-superusers"},"Enable authorization and assign superusers"),(0,o.kt)("p",null,"You can enable the authorization and assign the superusers in the broker (",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/reference-configuration#broker"},(0,o.kt)("inlineCode",{parentName:"a"},"conf/broker.conf")),") configuration files."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-properties"},"\nauthorizationEnabled=true\nsuperUserRoles=my-super-user-1,my-super-user-2\n\n")),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"A full list of parameters is available in the ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/broker.conf")," file.\nYou can also find the default values for those parameters in ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/reference-configuration#broker"},"Broker Configuration"),". ")),(0,o.kt)("p",null,"Typically, you use superuser roles for administrators, clients as well as broker-to-broker authorization. When you use ",(0,o.kt)("a",{parentName:"p",href:"concepts-replication"},"geo-replication"),", every broker needs to be able to publish to all the other topics of clusters."),(0,o.kt)("p",null,"You can also enable the authorization for the proxy in the proxy configuration file (",(0,o.kt)("inlineCode",{parentName:"p"},"conf/proxy.conf"),"). Once you enable the authorization on the proxy, the proxy does an additional authorization check before forwarding the request to a broker.\nIf you enable authorization on the broker, the broker checks the authorization of the request when the broker receives the forwarded request."),(0,o.kt)("h3",{id:"proxy-roles"},"Proxy Roles"),(0,o.kt)("p",null,"By default, the broker treats the connection between a proxy and the broker as a normal user connection. The broker authenticates the user as the role configured in ",(0,o.kt)("inlineCode",{parentName:"p"},"proxy.conf"),"(see ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/security-tls-authentication#enable-tls-authentication-on-proxies"},'"Enable TLS Authentication on Proxies"'),"). However, when the user connects to the cluster through a proxy, the user rarely requires the authentication. The user expects to be able to interact with the cluster as the role for which they have authenticated with the proxy."),(0,o.kt)("p",null,"Pulsar uses ",(0,o.kt)("em",{parentName:"p"},"Proxy roles")," to enable the authentication. Proxy roles are specified in the broker configuration file, ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/reference-configuration#broker"},(0,o.kt)("inlineCode",{parentName:"a"},"conf/broker.conf")),". If a client that is authenticated with a broker is one of its ",(0,o.kt)("inlineCode",{parentName:"p"},"proxyRoles"),", all requests from that client must also carry information about the role of the client that is authenticated with the proxy. This information is called the ",(0,o.kt)("em",{parentName:"p"},"original principal"),". If the ",(0,o.kt)("em",{parentName:"p"},"original principal")," is absent, the client is not able to access anything."),(0,o.kt)("p",null,"You must authorize both the ",(0,o.kt)("em",{parentName:"p"},"proxy role")," and the ",(0,o.kt)("em",{parentName:"p"},"original principal")," to access a resource to ensure that the resource is accessible via the proxy. Administrators can take two approaches to authorize the ",(0,o.kt)("em",{parentName:"p"},"proxy role")," and the ",(0,o.kt)("em",{parentName:"p"},"original principal"),"."),(0,o.kt)("p",null,"The more secure approach is to grant access to the proxy roles each time you grant access to a resource. For example, if you have a proxy role named ",(0,o.kt)("inlineCode",{parentName:"p"},"proxy1"),", when the superuser creates a tenant, you should specify ",(0,o.kt)("inlineCode",{parentName:"p"},"proxy1")," as one of the admin roles. When a role is granted permissions to produce or consume from a namespace, if that client wants to produce or consume through a proxy, you should also grant ",(0,o.kt)("inlineCode",{parentName:"p"},"proxy1")," the same permissions."),(0,o.kt)("p",null,"Another approach is to make the proxy role a superuser. This allows the proxy to access all resources. The client still needs to authenticate with the proxy, and all requests made through the proxy have their role downgraded to the ",(0,o.kt)("em",{parentName:"p"},"original principal")," of the authenticated client. However, if the proxy is compromised, a bad actor could get full access to your cluster."),(0,o.kt)("p",null,"You can specify the roles as proxy roles in ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/reference-configuration#broker"},(0,o.kt)("inlineCode",{parentName:"a"},"conf/broker.conf")),"."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-properties"},"\nproxyRoles=my-proxy-role\n\n# if you want to allow superusers to use the proxy (see above)\nsuperUserRoles=my-super-user-1,my-super-user-2,my-proxy-role\n\n")),(0,o.kt)("h2",{id:"administer-tenants"},"Administer tenants"),(0,o.kt)("p",null,"Pulsar ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/reference-terminology#instance"},"instance")," administrators or some kind of self-service portal typically provisions a Pulsar ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.7.0/reference-terminology#tenant"},"tenant"),". "),(0,o.kt)("p",null,"You can manage tenants using the ",(0,o.kt)("a",{parentName:"p",href:"reference-pulsar-admin"},(0,o.kt)("inlineCode",{parentName:"a"},"pulsar-admin"))," tool. "),(0,o.kt)("h3",{id:"create-a-new-tenant"},"Create a new tenant"),(0,o.kt)("p",null,"The following is an example tenant creation command:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ bin/pulsar-admin tenants create my-tenant \\\n  --admin-roles my-admin-role \\\n  --allowed-clusters us-west,us-east\n\n")),(0,o.kt)("p",null,"This command creates a new tenant ",(0,o.kt)("inlineCode",{parentName:"p"},"my-tenant")," that is allowed to use the clusters ",(0,o.kt)("inlineCode",{parentName:"p"},"us-west")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"us-east"),"."),(0,o.kt)("p",null,"A client that successfully identifies itself as having the role ",(0,o.kt)("inlineCode",{parentName:"p"},"my-admin-role")," is allowed to perform all administrative tasks on this tenant."),(0,o.kt)("p",null,"The structure of topic names in Pulsar reflects the hierarchy between tenants, clusters, and namespaces:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\npersistent://tenant/namespace/topic\n\n")),(0,o.kt)("h3",{id:"manage-permissions"},"Manage permissions"),(0,o.kt)("p",null,"You can use ",(0,o.kt)("a",{parentName:"p",href:"admin-api-permissions"},"Pulsar Admin Tools")," for managing permission in Pulsar."),(0,o.kt)("h3",{id:"pulsar-admin-authentication"},"Pulsar admin authentication"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},'\nPulsarAdmin admin = PulsarAdmin.builder()\n                    .serviceHttpUrl("http://broker:8080")\n                    .authentication("com.org.MyAuthPluginClass", "param1:value1")\n                    .build();\n\n')),(0,o.kt)("p",null,"To use TLS:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},'\nPulsarAdmin admin = PulsarAdmin.builder()\n                    .serviceHttpUrl("https://broker:8080")\n                    .authentication("com.org.MyAuthPluginClass", "param1:value1")\n                    .tlsTrustCertsFilePath("/path/to/trust/cert")\n                    .build();\n\n')))}h.isMDXComponent=!0},86010:function(e,t,n){function r(e){var t,n,a="";if("string"==typeof e||"number"==typeof e)a+=e;else if("object"==typeof e)if(Array.isArray(e))for(t=0;t<e.length;t++)e[t]&&(n=r(e[t]))&&(a&&(a+=" "),a+=n);else for(t in e)e[t]&&(a&&(a+=" "),a+=t);return a}function a(){for(var e,t,n=0,a="";n<arguments.length;)(e=arguments[n++])&&(t=r(e))&&(a&&(a+=" "),a+=t);return a}n.d(t,{Z:function(){return a}})}}]);
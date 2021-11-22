"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[94406],{3905:function(e,t,n){n.d(t,{Zo:function(){return c},kt:function(){return m}});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},l=Object.keys(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var u=r.createContext({}),s=function(e){var t=r.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},c=function(e){var t=s(e.components);return r.createElement(u.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,l=e.originalType,u=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),d=s(n),m=a,f=d["".concat(u,".").concat(m)]||d[m]||p[m]||l;return n?r.createElement(f,o(o({ref:t},c),{},{components:n})):r.createElement(f,o({ref:t},c))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var l=n.length,o=new Array(l);o[0]=d;var i={};for(var u in t)hasOwnProperty.call(t,u)&&(i[u]=t[u]);i.originalType=e,i.mdxType="string"==typeof e?e:a,o[1]=i;for(var s=2;s<l;s++)o[s]=n[s];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},58215:function(e,t,n){var r=n(67294);t.Z=function(e){var t=e.children,n=e.hidden,a=e.className;return r.createElement("div",{role:"tabpanel",hidden:n,className:a},t)}},55064:function(e,t,n){n.d(t,{Z:function(){return s}});var r=n(67294),a=n(79443);var l=function(){var e=(0,r.useContext)(a.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e},o=n(86010),i="tabItem_1uMI",u="tabItemActive_2DSg";var s=function(e){var t,n=e.lazy,a=e.block,s=e.defaultValue,c=e.values,p=e.groupId,d=e.className,m=r.Children.toArray(e.children),f=null!=c?c:m.map((function(e){return{value:e.props.value,label:e.props.label}})),h=null!=s?s:null==(t=m.find((function(e){return e.props.default})))?void 0:t.props.value,v=l(),b=v.tabGroupChoices,g=v.setTabGroupChoices,y=(0,r.useState)(h),k=y[0],w=y[1],N=[];if(null!=p){var O=b[p];null!=O&&O!==k&&f.some((function(e){return e.value===O}))&&w(O)}var P=function(e){var t=e.currentTarget,n=N.indexOf(t),r=f[n].value;w(r),null!=p&&(g(p,r),setTimeout((function(){var e,n,r,a,l,o,i,s;(e=t.getBoundingClientRect(),n=e.top,r=e.left,a=e.bottom,l=e.right,o=window,i=o.innerHeight,s=o.innerWidth,n>=0&&l<=s&&a<=i&&r>=0)||(t.scrollIntoView({block:"center",behavior:"smooth"}),t.classList.add(u),setTimeout((function(){return t.classList.remove(u)}),2e3))}),150))},x=function(e){var t,n=null;switch(e.key){case"ArrowRight":var r=N.indexOf(e.target)+1;n=N[r]||N[0];break;case"ArrowLeft":var a=N.indexOf(e.target)-1;n=N[a]||N[N.length-1]}null==(t=n)||t.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.Z)("tabs",{"tabs--block":a},d)},f.map((function(e){var t=e.value,n=e.label;return r.createElement("li",{role:"tab",tabIndex:k===t?0:-1,"aria-selected":k===t,className:(0,o.Z)("tabs__item",i,{"tabs__item--active":k===t}),key:t,ref:function(e){return N.push(e)},onKeyDown:x,onFocus:P,onClick:P},null!=n?n:t)}))),n?(0,r.cloneElement)(m.filter((function(e){return e.props.value===k}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},m.map((function(e,t){return(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==k})}))))}},79443:function(e,t,n){var r=(0,n(67294).createContext)(void 0);t.Z=r},241:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return i},contentTitle:function(){return u},metadata:function(){return s},toc:function(){return c},default:function(){return d}});var r=n(87462),a=n(63366),l=(n(67294),n(3905)),o=(n(55064),n(58215),["components"]),i={id:"helm-install",title:"Install Apache Pulsar using Helm",sidebar_label:"Install",original_id:"helm-install"},u=void 0,s={unversionedId:"helm-install",id:"version-2.7.0/helm-install",isDocsHomePage:!1,title:"Install Apache Pulsar using Helm",description:"Install Apache Pulsar on Kubernetes with the official Pulsar Helm chart.",source:"@site/versioned_docs/version-2.7.0/helm-install.md",sourceDirName:".",slug:"/helm-install",permalink:"/docs/2.7.0/helm-install",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.0/helm-install.md",tags:[],version:"2.7.0",frontMatter:{id:"helm-install",title:"Install Apache Pulsar using Helm",sidebar_label:"Install",original_id:"helm-install"},sidebar:"version-2.7.0/docsSidebar",previous:{title:"Prepare",permalink:"/docs/2.7.0/helm-prepare"},next:{title:"Deployment",permalink:"/docs/2.7.0/helm-deploy"}},c=[{value:"Requirements",id:"requirements",children:[]},{value:"Environment setup",id:"environment-setup",children:[{value:"Tools",id:"tools",children:[]}]},{value:"Cloud cluster preparation",id:"cloud-cluster-preparation",children:[]},{value:"Pulsar deployment",id:"pulsar-deployment",children:[]},{value:"Pulsar upgrade",id:"pulsar-upgrade",children:[]}],p={toc:c};function d(e){var t=e.components,n=(0,a.Z)(e,o);return(0,l.kt)("wrapper",(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("p",null,"Install Apache Pulsar on Kubernetes with the official Pulsar Helm chart."),(0,l.kt)("h2",{id:"requirements"},"Requirements"),(0,l.kt)("p",null,"To deploy Apache Pulsar on Kubernetes, the followings are required."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"kubectl 1.14 or higher, compatible with your cluster (",(0,l.kt)("a",{parentName:"li",href:"https://kubernetes.io/docs/tasks/tools/install-kubectl/#before-you-begin"},"+/- 1 minor release from your cluster"),")"),(0,l.kt)("li",{parentName:"ul"},"Helm v3 (3.0.2 or higher)"),(0,l.kt)("li",{parentName:"ul"},"A Kubernetes cluster, version 1.14 or higher")),(0,l.kt)("h2",{id:"environment-setup"},"Environment setup"),(0,l.kt)("p",null,"Before deploying Pulsar, you need to prepare your environment."),(0,l.kt)("h3",{id:"tools"},"Tools"),(0,l.kt)("p",null,"Install ",(0,l.kt)("a",{parentName:"p",href:"/docs/2.7.0/helm-tools"},(0,l.kt)("inlineCode",{parentName:"a"},"helm"))," and ",(0,l.kt)("a",{parentName:"p",href:"helm-tools"},(0,l.kt)("inlineCode",{parentName:"a"},"kubectl"))," on your computer."),(0,l.kt)("h2",{id:"cloud-cluster-preparation"},"Cloud cluster preparation"),(0,l.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,l.kt)("div",{parentName:"div",className:"admonition-heading"},(0,l.kt)("h5",{parentName:"div"},(0,l.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,l.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,l.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,l.kt)("div",{parentName:"div",className:"admonition-content"},(0,l.kt)("p",{parentName:"div"},"Kubernetes 1.14 or higher is required."))),(0,l.kt)("p",null,"To create and connect to the Kubernetes cluster, follow the instructions:"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("a",{parentName:"li",href:"/docs/2.7.0/helm-prepare#google-kubernetes-engine"},"Google Kubernetes Engine"))),(0,l.kt)("h2",{id:"pulsar-deployment"},"Pulsar deployment"),(0,l.kt)("p",null,"Once the environment is set up and configuration is generated, you can now proceed to the ",(0,l.kt)("a",{parentName:"p",href:"helm-deploy"},"deployment of Pulsar"),"."),(0,l.kt)("h2",{id:"pulsar-upgrade"},"Pulsar upgrade"),(0,l.kt)("p",null,"To upgrade an existing Kubernetes installation, follow the ",(0,l.kt)("a",{parentName:"p",href:"helm-upgrade"},"upgrade documentation"),"."))}d.isMDXComponent=!0},86010:function(e,t,n){function r(e){var t,n,a="";if("string"==typeof e||"number"==typeof e)a+=e;else if("object"==typeof e)if(Array.isArray(e))for(t=0;t<e.length;t++)e[t]&&(n=r(e[t]))&&(a&&(a+=" "),a+=n);else for(t in e)e[t]&&(a&&(a+=" "),a+=t);return a}function a(){for(var e,t,n=0,a="";n<arguments.length;)(e=arguments[n++])&&(t=r(e))&&(a&&(a+=" "),a+=t);return a}n.d(t,{Z:function(){return a}})}}]);
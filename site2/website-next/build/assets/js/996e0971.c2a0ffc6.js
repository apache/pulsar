"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[78041],{3905:function(e,t,n){n.d(t,{Zo:function(){return u},kt:function(){return d}});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var p=r.createContext({}),s=function(e){var t=r.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},u=function(e){var t=s(e.components);return r.createElement(p.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,p=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),m=s(n),d=a,h=m["".concat(p,".").concat(d)]||m[d]||c[d]||o;return n?r.createElement(h,l(l({ref:t},u),{},{components:n})):r.createElement(h,l({ref:t},u))}));function d(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,l=new Array(o);l[0]=m;var i={};for(var p in t)hasOwnProperty.call(t,p)&&(i[p]=t[p]);i.originalType=e,i.mdxType="string"==typeof e?e:a,l[1]=i;for(var s=2;s<o;s++)l[s]=n[s];return r.createElement.apply(null,l)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},48649:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return i},contentTitle:function(){return p},metadata:function(){return s},toc:function(){return u},default:function(){return m}});var r=n(87462),a=n(63366),o=(n(67294),n(3905)),l=["components"],i={id:"helm-upgrade",title:"Upgrade Pulsar Helm release",sidebar_label:"Upgrade",original_id:"helm-upgrade"},p=void 0,s={unversionedId:"helm-upgrade",id:"version-2.7.2/helm-upgrade",isDocsHomePage:!1,title:"Upgrade Pulsar Helm release",description:"Before upgrading your Pulsar installation, you need to check the change log corresponding to the specific release you want to upgrade to and look for any release notes that might pertain to the new Pulsar helm chart version.",source:"@site/versioned_docs/version-2.7.2/helm-upgrade.md",sourceDirName:".",slug:"/helm-upgrade",permalink:"/docs/2.7.2/helm-upgrade",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.2/helm-upgrade.md",tags:[],version:"2.7.2",frontMatter:{id:"helm-upgrade",title:"Upgrade Pulsar Helm release",sidebar_label:"Upgrade",original_id:"helm-upgrade"},sidebar:"version-2.7.2/docsSidebar",previous:{title:"Deployment",permalink:"/docs/2.7.2/helm-deploy"},next:{title:"Required Tools",permalink:"/docs/2.7.2/helm-tools"}},u=[{value:"Steps",id:"steps",children:[]}],c={toc:u};function m(e){var t=e.components,n=(0,a.Z)(e,l);return(0,o.kt)("wrapper",(0,r.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"Before upgrading your Pulsar installation, you need to check the change log corresponding to the specific release you want to upgrade to and look for any release notes that might pertain to the new Pulsar helm chart version."),(0,o.kt)("p",null,"We also recommend that you need to provide all values using the ",(0,o.kt)("inlineCode",{parentName:"p"},"helm upgrade --set key=value")," syntax or the ",(0,o.kt)("inlineCode",{parentName:"p"},"-f values.yml")," instead of using ",(0,o.kt)("inlineCode",{parentName:"p"},"--reuse-values"),", because some of the current values might be deprecated."),(0,o.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"You can retrieve your previous ",(0,o.kt)("inlineCode",{parentName:"p"},"--set")," arguments cleanly, with ",(0,o.kt)("inlineCode",{parentName:"p"},"helm get values <release-name>"),". If you direct this into a file (",(0,o.kt)("inlineCode",{parentName:"p"},"helm get values <release-name> > pulsar.yml"),"), you can safely"))),(0,o.kt)("p",null,"pass this file through ",(0,o.kt)("inlineCode",{parentName:"p"},"-f"),". Thus ",(0,o.kt)("inlineCode",{parentName:"p"},"helm upgrade <release-name> apache/pulsar -f pulsar.yaml"),". This safely replaces the behavior of ",(0,o.kt)("inlineCode",{parentName:"p"},"--reuse-values"),"."),(0,o.kt)("h2",{id:"steps"},"Steps"),(0,o.kt)("p",null,"To upgrade Apache Pulsar to a newer version, follow these steps:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Check the change log for the specific version you would like to upgrade to.")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Go through ",(0,o.kt)("a",{parentName:"p",href:"helm-deploy"},"deployment documentation")," step by step.")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Extract your previous ",(0,o.kt)("inlineCode",{parentName:"p"},"--set")," arguments with the following command."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nhelm get values <release-name> > pulsar.yaml\n\n"))),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Decide all the values you need to set.")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Perform the upgrade, with all ",(0,o.kt)("inlineCode",{parentName:"p"},"--set")," arguments extracted in step 4."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nhelm upgrade <release-name> apache/pulsar \\\n    --version <new version> \\\n    -f pulsar.yaml \\\n    --set ...\n\n")))))}m.isMDXComponent=!0}}]);
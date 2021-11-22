"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[86817],{3905:function(e,t,n){n.d(t,{Zo:function(){return c},kt:function(){return m}});var r=n(67294);function l(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){l(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,l=function(e,t){if(null==e)return{};var n,r,l={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(l[n]=e[n]);return l}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(l[n]=e[n])}return l}var p=r.createContext({}),s=function(e){var t=r.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},c=function(e){var t=s(e.components);return r.createElement(p.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,l=e.mdxType,o=e.originalType,p=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),d=s(n),m=l,g=d["".concat(p,".").concat(m)]||d[m]||u[m]||o;return n?r.createElement(g,a(a({ref:t},c),{},{components:n})):r.createElement(g,a({ref:t},c))}));function m(e,t){var n=arguments,l=t&&t.mdxType;if("string"==typeof e||l){var o=n.length,a=new Array(o);a[0]=d;var i={};for(var p in t)hasOwnProperty.call(t,p)&&(i[p]=t[p]);i.originalType=e,i.mdxType="string"==typeof e?e:l,a[1]=i;for(var s=2;s<o;s++)a[s]=n[s];return r.createElement.apply(null,a)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},34740:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return i},contentTitle:function(){return p},metadata:function(){return s},toc:function(){return c},default:function(){return d}});var r=n(87462),l=n(63366),o=(n(67294),n(3905)),a=["components"],i={id:"develop-cpp",title:"Building Pulsar C++ client",sidebar_label:"Building Pulsar C++ client",original_id:"develop-cpp"},p=void 0,s={unversionedId:"develop-cpp",id:"version-2.6.4/develop-cpp",isDocsHomePage:!1,title:"Building Pulsar C++ client",description:"Supported platforms",source:"@site/versioned_docs/version-2.6.4/developing-cpp.md",sourceDirName:".",slug:"/develop-cpp",permalink:"/docs/2.6.4/develop-cpp",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.4/developing-cpp.md",tags:[],version:"2.6.4",frontMatter:{id:"develop-cpp",title:"Building Pulsar C++ client",sidebar_label:"Building Pulsar C++ client",original_id:"develop-cpp"},sidebar:"version-2.6.4/docsSidebar",previous:{title:"Modular load manager",permalink:"/docs/2.6.4/develop-load-manager"},next:{title:"Terminology",permalink:"/docs/2.6.4/reference-terminology"}},c=[{value:"Supported platforms",id:"supported-platforms",children:[]},{value:"System requirements",id:"system-requirements",children:[]},{value:"Compilation",id:"compilation",children:[{value:"Linux",id:"linux",children:[]},{value:"MacOS",id:"macos",children:[]}]}],u={toc:c};function d(e){var t=e.components,n=(0,l.Z)(e,a);return(0,o.kt)("wrapper",(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"supported-platforms"},"Supported platforms"),(0,o.kt)("p",null,"The Pulsar C++ client has been successfully tested on ",(0,o.kt)("strong",{parentName:"p"},"MacOS")," and ",(0,o.kt)("strong",{parentName:"p"},"Linux"),"."),(0,o.kt)("h2",{id:"system-requirements"},"System requirements"),(0,o.kt)("p",null,"You need to have the following installed to use the C++ client:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://cmake.org/"},"CMake")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"http://www.boost.org/"},"Boost")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://developers.google.com/protocol-buffers/"},"Protocol Buffers")," 2.6"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://logging.apache.org/log4cxx"},"Log4CXX")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://curl.haxx.se/libcurl/"},"libcurl")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://github.com/google/googletest"},"Google Test")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://github.com/open-source-parsers/jsoncpp"},"JsonCpp"))),(0,o.kt)("h2",{id:"compilation"},"Compilation"),(0,o.kt)("p",null,"There are separate compilation instructions for ",(0,o.kt)("a",{parentName:"p",href:"#macos"},"MacOS")," and ",(0,o.kt)("a",{parentName:"p",href:"#linux"},"Linux"),". For both systems, start by cloning the Pulsar repository:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ git clone https://github.com/apache/pulsar\n\n")),(0,o.kt)("h3",{id:"linux"},"Linux"),(0,o.kt)("p",null,"First, install all of the necessary dependencies:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ apt-get install cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \\\n  libprotobuf-dev protobuf-compiler libboost-all-dev google-mock libgtest-dev libjsoncpp-dev\n\n")),(0,o.kt)("p",null,"Then compile and install ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/google/googletest"},"Google Test"),":"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n# libgtest-dev version is 1.18.0 or above\n$ cd /usr/src/googletest\n$ sudo cmake .\n$ sudo make\n$ sudo cp ./googlemock/libgmock.a ./googlemock/gtest/libgtest.a /usr/lib/\n\n# less than 1.18.0\n$ cd /usr/src/gtest\n$ sudo cmake .\n$ sudo make\n$ sudo cp libgtest.a /usr/lib\n\n$ cd /usr/src/gmock\n$ sudo cmake .\n$ sudo make\n$ sudo cp libgmock.a /usr/lib\n\n")),(0,o.kt)("p",null,"Finally, compile the Pulsar client library for C++ inside the Pulsar repo:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ cd pulsar-client-cpp\n$ cmake .\n$ make\n\n")),(0,o.kt)("p",null,"The resulting files, ",(0,o.kt)("inlineCode",{parentName:"p"},"libpulsar.so")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"libpulsar.a"),", will be placed in the ",(0,o.kt)("inlineCode",{parentName:"p"},"lib")," folder of the repo while two tools, ",(0,o.kt)("inlineCode",{parentName:"p"},"perfProducer")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"perfConsumer"),", will be placed in the ",(0,o.kt)("inlineCode",{parentName:"p"},"perf")," directory."),(0,o.kt)("h3",{id:"macos"},"MacOS"),(0,o.kt)("p",null,"First, install all of the necessary dependencies:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n# OpenSSL installation\n$ brew install openssl\n$ export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include/\n$ export OPENSSL_ROOT_DIR=/usr/local/opt/openssl/\n\n# Protocol Buffers installation\n$ brew tap homebrew/versions\n$ brew install protobuf260\n$ brew install boost\n$ brew install log4cxx\n\n# Google Test installation\n$ git clone https://github.com/google/googletest.git\n$ cd googletest\n$ cmake .\n$ make install\n\n")),(0,o.kt)("p",null,"Then compile the Pulsar client library in the repo that you cloned:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\n$ cd pulsar-client-cpp\n$ cmake .\n$ make\n\n")))}d.isMDXComponent=!0}}]);
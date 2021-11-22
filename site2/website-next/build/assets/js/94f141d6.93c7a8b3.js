"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[21577],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return d}});var r=n(67294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),u=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=u(e.components);return r.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},k=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),k=u(n),d=o,m=k["".concat(s,".").concat(d)]||k[d]||c[d]||a;return n?r.createElement(m,i(i({ref:t},p),{},{components:n})):r.createElement(m,i({ref:t},p))}));function d(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,i=new Array(a);i[0]=k;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var u=2;u<a;u++)i[u]=n[u];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}k.displayName="MDXCreateElement"},48457:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return l},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return p},default:function(){return k}});var r=n(87462),o=n(63366),a=(n(67294),n(3905)),i=["components"],l={id:"functions-worker",title:"Deploy and manage functions worker",sidebar_label:"Setup: Pulsar Functions Worker",original_id:"functions-worker"},s=void 0,u={unversionedId:"functions-worker",id:"version-2.6.3/functions-worker",isDocsHomePage:!1,title:"Deploy and manage functions worker",description:"Before using Pulsar Functions, you need to learn how to set up Pulsar Functions worker and how to configure Functions runtime.",source:"@site/versioned_docs/version-2.6.3/functions-worker.md",sourceDirName:".",slug:"/functions-worker",permalink:"/docs/2.6.3/functions-worker",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.3/functions-worker.md",tags:[],version:"2.6.3",frontMatter:{id:"functions-worker",title:"Deploy and manage functions worker",sidebar_label:"Setup: Pulsar Functions Worker",original_id:"functions-worker"},sidebar:"version-2.6.3/docsSidebar",previous:{title:"Overview",permalink:"/docs/2.6.3/functions-overview"},next:{title:"Setup: Configure Functions runtime",permalink:"/docs/2.6.3/functions-runtime"}},p=[{value:"Run Functions-worker with brokers",id:"run-functions-worker-with-brokers",children:[{value:"Configure Functions-Worker to run with brokers",id:"configure-functions-worker-to-run-with-brokers",children:[]},{value:"Configure Stateful-Functions to run with broker",id:"configure-stateful-functions-to-run-with-broker",children:[]},{value:"Start Functions-worker with broker",id:"start-functions-worker-with-broker",children:[]}]},{value:"Run Functions-worker separately",id:"run-functions-worker-separately",children:[{value:"Configure Functions-worker to run separately",id:"configure-functions-worker-to-run-separately",children:[]},{value:"Start Functions-worker",id:"start-functions-worker",children:[]},{value:"Configure Proxies for Functions-workers",id:"configure-proxies-for-functions-workers",children:[]}]},{value:"Compare the Run-with-Broker and Run-separately modes",id:"compare-the-run-with-broker-and-run-separately-modes",children:[]},{value:"Troubleshooting",id:"troubleshooting",children:[]}],c={toc:p};function k(e){var t=e.components,l=(0,o.Z)(e,i);return(0,a.kt)("wrapper",(0,r.Z)({},c,l,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"Before using Pulsar Functions, you need to learn how to set up Pulsar Functions worker and how to ",(0,a.kt)("a",{parentName:"p",href:"functions-runtime"},"configure Functions runtime"),".  "),(0,a.kt)("p",null,"Pulsar ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," is a logic component to run Pulsar Functions in cluster mode. Two options are available, and you can select either based on your requirements. "),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"#run-functions-worker-with-brokers"},"run with brokers")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"#run-functions-worker-separately"},"run it separately")," in a different broker")),(0,a.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,a.kt)("div",{parentName:"div",className:"admonition-heading"},(0,a.kt)("h5",{parentName:"div"},(0,a.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,a.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,a.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,a.kt)("div",{parentName:"div",className:"admonition-content"},(0,a.kt)("p",{parentName:"div"},"The ",(0,a.kt)("inlineCode",{parentName:"p"},"--- Service Urls---")," lines in the following diagrams represent Pulsar service URLs that Pulsar client and admin use to connect to a Pulsar cluster."))),(0,a.kt)("h2",{id:"run-functions-worker-with-brokers"},"Run Functions-worker with brokers"),(0,a.kt)("p",null,"The following diagram illustrates the deployment of functions-workers running along with brokers."),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"assets/functions-worker-corun.png",src:n(46872).Z})),(0,a.kt)("p",null,"To enable functions-worker running as part of a broker, you need to set ",(0,a.kt)("inlineCode",{parentName:"p"},"functionsWorkerEnabled")," to ",(0,a.kt)("inlineCode",{parentName:"p"},"true")," in the ",(0,a.kt)("inlineCode",{parentName:"p"},"broker.conf")," file."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-conf"},"\nfunctionsWorkerEnabled=true\n\n")),(0,a.kt)("p",null,"If the ",(0,a.kt)("inlineCode",{parentName:"p"},"functionsWorkerEnabled")," is set to ",(0,a.kt)("inlineCode",{parentName:"p"},"true"),", the functions-worker is started as part of a broker. You need to configure the ",(0,a.kt)("inlineCode",{parentName:"p"},"conf/functions_worker.yml")," file to customize your functions_worker."),(0,a.kt)("p",null,"Before you run Functions-worker with broker, you have to configure Functions-worker, and then start it with brokers."),(0,a.kt)("h3",{id:"configure-functions-worker-to-run-with-brokers"},"Configure Functions-Worker to run with brokers"),(0,a.kt)("p",null,"In this mode, most of the settings are already inherited from your broker configuration (for example, configurationStore settings, authentication settings, and so on) since ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," is running as part of the broker."),(0,a.kt)("p",null,"Pay attention to the following required settings when configuring functions-worker in this mode."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"numFunctionPackageReplicas"),": The number of replicas to store function packages. The default value is ",(0,a.kt)("inlineCode",{parentName:"li"},"1"),", which is good for standalone deployment. For production deployment, to ensure high availability, set it to be larger than ",(0,a.kt)("inlineCode",{parentName:"li"},"2"),"."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"pulsarFunctionsCluster"),": Set the value to your Pulsar cluster name (same as the ",(0,a.kt)("inlineCode",{parentName:"li"},"clusterName")," setting in the broker configuration).")),(0,a.kt)("p",null,"If authentication is enabled on the BookKeeper cluster, configure the following BookKeeper authentication settings."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeperClientAuthenticationPlugin"),": the BookKeeper client authentication plugin name."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeperClientAuthenticationParametersName"),": the BookKeeper client authentication plugin parameters name."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeperClientAuthenticationParameters"),": the BookKeeper client authentication plugin parameters.")),(0,a.kt)("h3",{id:"configure-stateful-functions-to-run-with-broker"},"Configure Stateful-Functions to run with broker"),(0,a.kt)("p",null,"If you want to use Stateful-Functions related functions (for example,  ",(0,a.kt)("inlineCode",{parentName:"p"},"putState()")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"queryState()")," related interfaces), follow steps below."),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"Enable the ",(0,a.kt)("strong",{parentName:"p"},"streamStorage")," service in the BookKeeper."),(0,a.kt)("p",{parentName:"li"},"Currently, the service uses the NAR package, so you need to set the configuration in ",(0,a.kt)("inlineCode",{parentName:"p"},"bookkeeper.conf"),"."),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre",className:"language-text"},"\nextraServerComponents=org.apache.bookkeeper.stream.server.StreamStorageLifecycleComponent\n\n")),(0,a.kt)("p",{parentName:"li"},"After starting bookie, use the following methods to check whether the streamStorage service is started correctly."),(0,a.kt)("p",{parentName:"li"},"Input:"),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"\ntelnet localhost 4181\n\n")),(0,a.kt)("p",{parentName:"li"},"Output:"),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre",className:"language-text"},"\nTrying 127.0.0.1...\nConnected to localhost.\nEscape character is '^]'.\n\n"))),(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"Turn on this function in ",(0,a.kt)("inlineCode",{parentName:"p"},"functions_worker.yml"),"."),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre",className:"language-text"},"\nstateStorageServiceUrl: bk://<bk-service-url>:4181\n\n")),(0,a.kt)("p",{parentName:"li"},(0,a.kt)("inlineCode",{parentName:"p"},"bk-service-url")," is the service URL pointing to the BookKeeper table service."))),(0,a.kt)("h3",{id:"start-functions-worker-with-broker"},"Start Functions-worker with broker"),(0,a.kt)("p",null,"Once you have configured the ",(0,a.kt)("inlineCode",{parentName:"p"},"functions_worker.yml")," file, you can start or restart your broker. "),(0,a.kt)("p",null,"And then you can use the following command to verify if ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," is running well."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"\ncurl <broker-ip>:8080/admin/v2/worker/cluster\n\n")),(0,a.kt)("p",null,"After entering the command above, a list of active function workers in the cluster is returned. The output is similar to the following."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-json"},'\n[{"workerId":"<worker-id>","workerHostname":"<worker-hostname>","port":8080}]\n\n')),(0,a.kt)("h2",{id:"run-functions-worker-separately"},"Run Functions-worker separately"),(0,a.kt)("p",null,"This section illustrates how to run ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," as a separate process in separate machines."),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"assets/functions-worker-separated.png",src:n(52172).Z})),(0,a.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,a.kt)("div",{parentName:"div",className:"admonition-heading"},(0,a.kt)("h5",{parentName:"div"},(0,a.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,a.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,a.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,a.kt)("div",{parentName:"div",className:"admonition-content"},(0,a.kt)("p",{parentName:"div"},"In this mode, make sure ",(0,a.kt)("inlineCode",{parentName:"p"},"functionsWorkerEnabled")," is set to ",(0,a.kt)("inlineCode",{parentName:"p"},"false"),", so you won't start ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," with brokers by mistake."))),(0,a.kt)("h3",{id:"configure-functions-worker-to-run-separately"},"Configure Functions-worker to run separately"),(0,a.kt)("p",null,"To run function-worker separately, you have to configure the following parameters. "),(0,a.kt)("h4",{id:"worker-parameters"},"Worker parameters"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"workerId"),": The type is string. It is unique across clusters, which is used to identify a worker machine."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"workerHostname"),": The hostname of the worker machine."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"workerPort"),": The port that the worker server listens on. Keep it as default if you don't customize it."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"workerPortTls"),": The TLS port that the worker server listens on. Keep it as default if you don't customize it.")),(0,a.kt)("h4",{id:"function-package-parameter"},"Function package parameter"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"numFunctionPackageReplicas"),": The number of replicas to store function packages. The default value is ",(0,a.kt)("inlineCode",{parentName:"li"},"1"),".")),(0,a.kt)("h4",{id:"function-metadata-parameter"},"Function metadata parameter"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"pulsarServiceUrl"),": The Pulsar service URL for your broker cluster."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"pulsarWebServiceUrl"),": The Pulsar web service URL for your broker cluster."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"pulsarFunctionsCluster"),": Set the value to your Pulsar cluster name (same as the ",(0,a.kt)("inlineCode",{parentName:"li"},"clusterName")," setting in the broker configuration).")),(0,a.kt)("p",null,"If authentication is enabled for your broker cluster, you ",(0,a.kt)("em",{parentName:"p"},"should")," configure the authentication plugin and parameters for the functions worker to communicate with the brokers."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"clientAuthenticationPlugin")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"clientAuthenticationParameters"))),(0,a.kt)("h4",{id:"security-settings"},"Security settings"),(0,a.kt)("p",null,"If you want to enable security on functions workers, you ",(0,a.kt)("em",{parentName:"p"},"should"),":"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"#enable-tls-transport-encryption"},"Enable TLS transport encryption")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"#enable-authentication-provider"},"Enable Authentication Provider")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"#enable-authorization-provider"},"Enable Authorization Provider"))),(0,a.kt)("h5",{id:"enable-tls-transport-encryption"},"Enable TLS transport encryption"),(0,a.kt)("p",null,"To enable TLS transport encryption, configure the following settings."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\nuseTLS: true\npulsarServiceUrl: pulsar+ssl://localhost:6651/\npulsarWebServiceUrl: https://localhost:8443\n\ntlsEnabled: true\ntlsCertificateFilePath: /path/to/functions-worker.cert.pem\ntlsKeyFilePath:         /path/to/functions-worker.key-pk8.pem\ntlsTrustCertsFilePath:  /path/to/ca.cert.pem\n\n// The path to trusted certificates used by the Pulsar client to authenticate with Pulsar brokers\nbrokerClientTrustCertsFilePath: /path/to/ca.cert.pem\n\n")),(0,a.kt)("p",null,"For details on TLS encryption, refer to ",(0,a.kt)("a",{parentName:"p",href:"security-tls-transport"},"Transport Encryption using TLS"),"."),(0,a.kt)("h5",{id:"enable-authentication-provider"},"Enable Authentication Provider"),(0,a.kt)("p",null,"To enable authentication on Functions Worker, you need to configure the following settings."),(0,a.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,a.kt)("div",{parentName:"div",className:"admonition-heading"},(0,a.kt)("h5",{parentName:"div"},(0,a.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,a.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,a.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,a.kt)("div",{parentName:"div",className:"admonition-content"},(0,a.kt)("p",{parentName:"div"},"Substitute the ",(0,a.kt)("em",{parentName:"p"},"providers list")," with the providers you want to enable."))),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\nauthenticationEnabled: true\nauthenticationProviders: [ provider1, provider2 ]\n\n")),(0,a.kt)("p",null,"For ",(0,a.kt)("em",{parentName:"p"},"TLS Authentication")," provider, follow the example below to add the necessary settings.\nSee ",(0,a.kt)("a",{parentName:"p",href:"security-tls-authentication"},"TLS Authentication")," for more details."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\nbrokerClientAuthenticationPlugin: org.apache.pulsar.client.impl.auth.AuthenticationTls\nbrokerClientAuthenticationParameters: tlsCertFile:/path/to/admin.cert.pem,tlsKeyFile:/path/to/admin.key-pk8.pem\n\nauthenticationEnabled: true\nauthenticationProviders: ['org.apache.pulsar.broker.authentication.AuthenticationProviderTls']\n\n")),(0,a.kt)("p",null,"For ",(0,a.kt)("em",{parentName:"p"},"SASL Authentication")," provider, add ",(0,a.kt)("inlineCode",{parentName:"p"},"saslJaasClientAllowedIds")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"saslJaasBrokerSectionName"),"\nunder ",(0,a.kt)("inlineCode",{parentName:"p"},"properties")," if needed. "),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\nproperties:\n  saslJaasClientAllowedIds: .*pulsar.*\n  saslJaasBrokerSectionName: Broker\n\n")),(0,a.kt)("p",null,"For ",(0,a.kt)("em",{parentName:"p"},"Token Authentication")," provider, add necessary settings for ",(0,a.kt)("inlineCode",{parentName:"p"},"properties")," if needed.\nSee ",(0,a.kt)("a",{parentName:"p",href:"security-jwt"},"Token Authentication")," for more details."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\nproperties:\n  tokenSecretKey:       file://my/secret.key \n  # If using public/private\n  # tokenPublicKey:     file:///path/to/public.key\n\n")),(0,a.kt)("h5",{id:"enable-authorization-provider"},"Enable Authorization Provider"),(0,a.kt)("p",null,"To enable authorization on Functions Worker, you need to configure ",(0,a.kt)("inlineCode",{parentName:"p"},"authorizationEnabled"),", ",(0,a.kt)("inlineCode",{parentName:"p"},"authorizationProvider")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"configurationStoreServers"),". The authentication provider connects to ",(0,a.kt)("inlineCode",{parentName:"p"},"configurationStoreServers")," to receive namespace policies."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"\nauthorizationEnabled: true\nauthorizationProvider: org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider\nconfigurationStoreServers: <configuration-store-servers>\n\n")),(0,a.kt)("p",null,"You should also configure a list of superuser roles. The superuser roles are able to access any admin API. The following is a configuration example."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"\nsuperUserRoles:\n  - role1\n  - role2\n  - role3\n\n")),(0,a.kt)("h4",{id:"bookkeeper-authentication"},"BookKeeper Authentication"),(0,a.kt)("p",null,"If authentication is enabled on the BookKeeper cluster, you need configure the BookKeeper authentication settings as follows:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeperClientAuthenticationPlugin"),": the plugin name of BookKeeper client authentication."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeperClientAuthenticationParametersName"),": the plugin parameters name of BookKeeper client authentication."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeperClientAuthenticationParameters"),": the plugin parameters of BookKeeper client authentication.")),(0,a.kt)("h3",{id:"start-functions-worker"},"Start Functions-worker"),(0,a.kt)("p",null,"Once you have finished configuring the ",(0,a.kt)("inlineCode",{parentName:"p"},"functions_worker.yml")," configuration file, you can use the following command to start a ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker"),":"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar functions-worker\n\n")),(0,a.kt)("h3",{id:"configure-proxies-for-functions-workers"},"Configure Proxies for Functions-workers"),(0,a.kt)("p",null,"When you are running ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," in a separate cluster, the admin rest endpoints are split into two clusters. ",(0,a.kt)("inlineCode",{parentName:"p"},"functions"),", ",(0,a.kt)("inlineCode",{parentName:"p"},"function-worker"),", ",(0,a.kt)("inlineCode",{parentName:"p"},"source")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"sink")," endpoints are now served\nby the ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," cluster, while all the other remaining endpoints are served by the broker cluster.\nHence you need to configure your ",(0,a.kt)("inlineCode",{parentName:"p"},"pulsar-admin")," to use the right service URL accordingly."),(0,a.kt)("p",null,"In order to address this inconvenience, you can start a proxy cluster for routing the admin rest requests accordingly. Hence you will have one central entry point for your admin service."),(0,a.kt)("p",null,"If you already have a proxy cluster, continue reading. If you haven't setup a proxy cluster before, you can follow the ",(0,a.kt)("a",{parentName:"p",href:"http://pulsar.apache.org/docs/en/administration-proxy/"},"instructions")," to\nstart proxies.    "),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"assets/functions-worker-separated.png",src:n(35702).Z})),(0,a.kt)("p",null,"To enable routing functions related admin requests to ",(0,a.kt)("inlineCode",{parentName:"p"},"functions-worker")," in a proxy, you can edit the ",(0,a.kt)("inlineCode",{parentName:"p"},"proxy.conf")," file to modify the following settings:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-conf"},"\nfunctionWorkerWebServiceURL=<pulsar-functions-worker-web-service-url>\nfunctionWorkerWebServiceURLTLS=<pulsar-functions-worker-web-service-url>\n\n")),(0,a.kt)("h2",{id:"compare-the-run-with-broker-and-run-separately-modes"},"Compare the Run-with-Broker and Run-separately modes"),(0,a.kt)("p",null,"As described above, you can run Function-worker with brokers, or run it separately. And it is more convenient to run functions-workers along with brokers. However, running functions-workers in a separate cluster provides better resource isolation for running functions in ",(0,a.kt)("inlineCode",{parentName:"p"},"Process")," or ",(0,a.kt)("inlineCode",{parentName:"p"},"Thread")," mode."),(0,a.kt)("p",null,"Use which mode for your cases, refer to the following guidelines to determine."),(0,a.kt)("p",null,"Use the ",(0,a.kt)("inlineCode",{parentName:"p"},"Run-with-Broker")," mode in the following cases:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"a) if resource isolation is not required when running functions in ",(0,a.kt)("inlineCode",{parentName:"li"},"Process")," or ",(0,a.kt)("inlineCode",{parentName:"li"},"Thread")," mode; "),(0,a.kt)("li",{parentName:"ul"},"b) if you configure the functions-worker to run functions on Kubernetes (where the resource isolation problem is addressed by Kubernetes).")),(0,a.kt)("p",null,"Use the ",(0,a.kt)("inlineCode",{parentName:"p"},"Run-separately")," mode in the following cases:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"a) you don't have a Kubernetes cluster; "),(0,a.kt)("li",{parentName:"ul"},"b) if you want to run functions and brokers separately.")),(0,a.kt)("h2",{id:"troubleshooting"},"Troubleshooting"),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Error message: Namespace missing local cluster name in clusters list")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\nFailed to get partitioned topic metadata: org.apache.pulsar.client.api.PulsarClientException$BrokerMetadataException: Namespace missing local cluster name in clusters list: local_cluster=xyz ns=public/functions clusters=[standalone]\n\n")),(0,a.kt)("p",null,"The error message prompts when either of the cases occurs:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"a) a broker is started with ",(0,a.kt)("inlineCode",{parentName:"li"},"functionsWorkerEnabled=true"),", but the ",(0,a.kt)("inlineCode",{parentName:"li"},"pulsarFunctionsCluster")," is not set to the correct cluster in the ",(0,a.kt)("inlineCode",{parentName:"li"},"conf/functions_worker.yaml")," file;"),(0,a.kt)("li",{parentName:"ul"},"b) setting up a geo-replicated Pulsar cluster with ",(0,a.kt)("inlineCode",{parentName:"li"},"functionsWorkerEnabled=true"),", while brokers in one cluster run well, brokers in the other cluster do not work well.")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Workaround")),(0,a.kt)("p",null,"If any of these cases happens, follow the instructions below to fix the problem:"),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"Disable Functions Worker by setting ",(0,a.kt)("inlineCode",{parentName:"p"},"functionsWorkerEnabled=false"),", and restart brokers.")),(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"Get the current clusters list of ",(0,a.kt)("inlineCode",{parentName:"p"},"public/functions")," namespace."))),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin namespaces get-clusters public/functions\n\n")),(0,a.kt)("ol",{start:3},(0,a.kt)("li",{parentName:"ol"},"Check if the cluster is in the clusters list. If the cluster is not in the list, add it to the list and update the clusters list.")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin namespaces set-clusters --clusters <existing-clusters>,<new-cluster> public/functions\n\n")),(0,a.kt)("ol",{start:4},(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"After setting the cluster successfully, enable functions worker by setting ",(0,a.kt)("inlineCode",{parentName:"p"},"functionsWorkerEnabled=true"),". ")),(0,a.kt)("li",{parentName:"ol"},(0,a.kt)("p",{parentName:"li"},"Set the correct cluster name in ",(0,a.kt)("inlineCode",{parentName:"p"},"pulsarFunctionsCluster")," in the ",(0,a.kt)("inlineCode",{parentName:"p"},"conf/functions_worker.yml")," file, and restart brokers."))))}k.isMDXComponent=!0},46872:function(e,t,n){t.Z=n.p+"assets/images/functions-worker-corun-1e97464581d9ed837aad294946bd35f6.png"},35702:function(e,t,n){t.Z=n.p+"assets/images/functions-worker-separated-proxy-dbc4927f522e4ed19c925baca826e0cc.png"},52172:function(e,t,n){t.Z=n.p+"assets/images/functions-worker-separated-b484198781204f02277e700746966249.png"}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[91745],{3905:function(e,n,t){t.d(n,{Zo:function(){return l},kt:function(){return d}});var i=t(67294);function a(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function r(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);n&&(i=i.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,i)}return t}function o(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?r(Object(t),!0).forEach((function(n){a(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):r(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function s(e,n){if(null==e)return{};var t,i,a=function(e,n){if(null==e)return{};var t,i,a={},r=Object.keys(e);for(i=0;i<r.length;i++)t=r[i],n.indexOf(t)>=0||(a[t]=e[t]);return a}(e,n);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(i=0;i<r.length;i++)t=r[i],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var u=i.createContext({}),c=function(e){var n=i.useContext(u),t=n;return e&&(t="function"==typeof e?e(n):o(o({},n),e)),t},l=function(e){var n=c(e.components);return i.createElement(u.Provider,{value:n},e.children)},p={inlineCode:"code",wrapper:function(e){var n=e.children;return i.createElement(i.Fragment,{},n)}},m=i.forwardRef((function(e,n){var t=e.components,a=e.mdxType,r=e.originalType,u=e.parentName,l=s(e,["components","mdxType","originalType","parentName"]),m=c(t),d=a,h=m["".concat(u,".").concat(d)]||m[d]||p[d]||r;return t?i.createElement(h,o(o({ref:n},l),{},{components:t})):i.createElement(h,o({ref:n},l))}));function d(e,n){var t=arguments,a=n&&n.mdxType;if("string"==typeof e||a){var r=t.length,o=new Array(r);o[0]=m;var s={};for(var u in n)hasOwnProperty.call(n,u)&&(s[u]=n[u]);s.originalType=e,s.mdxType="string"==typeof e?e:a,o[1]=s;for(var c=2;c<r;c++)o[c]=t[c];return i.createElement.apply(null,o)}return i.createElement.apply(null,t)}m.displayName="MDXCreateElement"},7595:function(e,n,t){t.r(n),t.d(n,{frontMatter:function(){return s},contentTitle:function(){return u},metadata:function(){return c},toc:function(){return l},default:function(){return m}});var i=t(87462),a=t(63366),r=(t(67294),t(3905)),o=["components"],s={id:"functions-runtime",title:"Configure Functions runtime",sidebar_label:"Setup: Configure Functions runtime",original_id:"functions-runtime"},u=void 0,c={unversionedId:"functions-runtime",id:"version-2.7.3/functions-runtime",isDocsHomePage:!1,title:"Configure Functions runtime",description:"You can use the following methods to run functions.",source:"@site/versioned_docs/version-2.7.3/functions-runtime.md",sourceDirName:".",slug:"/functions-runtime",permalink:"/docs/2.7.3/functions-runtime",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.3/functions-runtime.md",tags:[],version:"2.7.3",frontMatter:{id:"functions-runtime",title:"Configure Functions runtime",sidebar_label:"Setup: Configure Functions runtime",original_id:"functions-runtime"},sidebar:"version-2.7.3/docsSidebar",previous:{title:"Setup: Pulsar Functions Worker",permalink:"/docs/2.7.3/functions-worker"},next:{title:"How-to: Develop",permalink:"/docs/2.7.3/functions-develop"}},l=[{value:"Configure thread runtime",id:"configure-thread-runtime",children:[]},{value:"Configure process runtime",id:"configure-process-runtime",children:[]},{value:"Configure Kubernetes runtime",id:"configure-kubernetes-runtime",children:[{value:"Basic configuration",id:"basic-configuration",children:[]},{value:"Run standalone functions worker on Kubernetes",id:"run-standalone-functions-worker-on-kubernetes",children:[]},{value:"Run RBAC in Kubernetes clusters",id:"run-rbac-in-kubernetes-clusters",children:[]},{value:"Integrate Kubernetes secrets",id:"integrate-kubernetes-secrets",children:[]},{value:"Enable token authentication",id:"enable-token-authentication",children:[]},{value:"Run clusters with authentication",id:"run-clusters-with-authentication",children:[]},{value:"Customize Kubernetes runtime",id:"customize-kubernetes-runtime",children:[]}]},{value:"Run clusters with geo-replication",id:"run-clusters-with-geo-replication",children:[]},{value:"Configure standalone functions worker",id:"configure-standalone-functions-worker",children:[]}],p={toc:l};function m(e){var n=e.components,t=(0,a.Z)(e,o);return(0,r.kt)("wrapper",(0,i.Z)({},p,t,{components:n,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"You can use the following methods to run functions."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("em",{parentName:"li"},"Thread"),": Invoke functions threads in functions worker."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("em",{parentName:"li"},"Process"),": Invoke functions in processes forked by functions worker."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("em",{parentName:"li"},"Kubernetes"),": Submit functions as Kubernetes StatefulSets by functions worker.")),(0,r.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"Pulsar supports adding labels to the Kubernetes StatefulSets and services while launching functions, which facilitates selecting the target Kubernetes objects."))),(0,r.kt)("p",null,"The differences of the thread and process modes are:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Thread mode: when a function runs in thread mode, it runs on the same Java virtual machine (JVM) with functions worker."),(0,r.kt)("li",{parentName:"ul"},"Process mode: when a function runs in process mode, it runs on the same machine that functions worker runs.")),(0,r.kt)("h2",{id:"configure-thread-runtime"},"Configure thread runtime"),(0,r.kt)("p",null,"It is easy to configure ",(0,r.kt)("em",{parentName:"p"},"Thread")," runtime. In most cases, you do not need to configure anything. You can customize the thread group name with the following settings:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'\nfunctionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory\nfunctionRuntimeFactoryConfigs:\n  threadGroupName: "Your Function Container Group"\n\n')),(0,r.kt)("p",null,(0,r.kt)("em",{parentName:"p"},"Thread")," runtime is only supported in Java function."),(0,r.kt)("h2",{id:"configure-process-runtime"},"Configure process runtime"),(0,r.kt)("p",null,"When you enable ",(0,r.kt)("em",{parentName:"p"},"Process")," runtime, you do not need to configure anything."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},"\nfunctionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory\nfunctionRuntimeFactoryConfigs:\n  # the directory for storing the function logs\n  logDirectory:\n  # change the jar location only when you put the java instance jar in a different location\n  javaInstanceJarLocation:\n  # change the python instance location only when you put the python instance jar in a different location\n  pythonInstanceLocation:\n  # change the extra dependencies location:\n  extraFunctionDependenciesDir:\n\n")),(0,r.kt)("p",null,(0,r.kt)("em",{parentName:"p"},"Process")," runtime is supported in Java, Python, and Go functions."),(0,r.kt)("h2",{id:"configure-kubernetes-runtime"},"Configure Kubernetes runtime"),(0,r.kt)("p",null,"When the functions worker generates Kubernetes manifests and apply the manifests, the Kubernetes runtime works. If you have run functions worker on Kubernetes, you can use the ",(0,r.kt)("inlineCode",{parentName:"p"},"serviceAccount")," associated with the pod that the functions worker is running in. Otherwise, you can configure it to communicate with a Kubernetes cluster."),(0,r.kt)("p",null,"The manifests, generated by the functions worker, include a ",(0,r.kt)("inlineCode",{parentName:"p"},"StatefulSet"),", a ",(0,r.kt)("inlineCode",{parentName:"p"},"Service")," (used to communicate with the pods), and a ",(0,r.kt)("inlineCode",{parentName:"p"},"Secret")," for auth credentials (when applicable). The ",(0,r.kt)("inlineCode",{parentName:"p"},"StatefulSet"),' manifest (by default) has a single pod, with the number of replicas determined by the "parallelism" of the function. On pod boot, the pod downloads the function payload (via the functions worker REST API). The pod\'s container image is configurable, but must have the functions runtime.'),(0,r.kt)("p",null,"The Kubernetes runtime supports secrets, so you can create a Kubernetes secret and expose it as an environment variable in the pod. The Kubernetes runtime is extensible, you can implement classes and customize the way how to generate Kubernetes manifests, how to pass auth data to pods, and how to integrate secrets."),(0,r.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"For the rules of translating Pulsar object names into Kubernetes resource labels, see ",(0,r.kt)("a",{parentName:"p",href:"/docs/2.7.3/admin-api-overview#how-to-define-pulsar-resource-names-when-running-pulsar-in-kubernetes"},"here"),"."))),(0,r.kt)("h3",{id:"basic-configuration"},"Basic configuration"),(0,r.kt)("p",null,"It is easy to configure Kubernetes runtime. You can just uncomment the settings of ",(0,r.kt)("inlineCode",{parentName:"p"},"kubernetesContainerFactory")," in the ",(0,r.kt)("inlineCode",{parentName:"p"},"functions_worker.yaml")," file. The following is an example."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},"\nfunctionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory\nfunctionRuntimeFactoryConfigs:\n  # uri to kubernetes cluster, leave it to empty and it will use the kubernetes settings in function worker\n  k8Uri:\n  # the kubernetes namespace to run the function instances. it is `default`, if this setting is left to be empty\n  jobNamespace:\n  # the docker image to run function instance. by default it is `apachepulsar/pulsar`\n  pulsarDockerImageName:\n  # the docker image to run function instance according to different configurations provided by users.\n  # By default it is `apachepulsar/pulsar`.\n  # e.g:\n  # functionDockerImages:\n  #   JAVA: JAVA_IMAGE_NAME\n  #   PYTHON: PYTHON_IMAGE_NAME\n  #   GO: GO_IMAGE_NAME\n  functionDockerImages:\n  # the root directory of pulsar home directory in `pulsarDockerImageName`. by default it is `/pulsar`.\n  # if you are using your own built image in `pulsarDockerImageName`, you need to set this setting accordingly\n  pulsarRootDir:\n  # this setting only takes effects if `k8Uri` is set to null. if your function worker is running as a k8 pod,\n  # setting this to true is let function worker to submit functions to the same k8s cluster as function worker\n  # is running. setting this to false if your function worker is not running as a k8 pod.\n  submittingInsidePod: false\n  # setting the pulsar service url that pulsar function should use to connect to pulsar\n  # if it is not set, it will use the pulsar service url configured in worker service\n  pulsarServiceUrl:\n  # setting the pulsar admin url that pulsar function should use to connect to pulsar\n  # if it is not set, it will use the pulsar admin url configured in worker service\n  pulsarAdminUrl:\n  # the custom labels that function worker uses to select the nodes for pods\n  customLabels:\n  # the directory for dropping extra function dependencies\n  # if it is not an absolute path, it is relative to `pulsarRootDir`\n  extraFunctionDependenciesDir:\n  # Additional memory padding added on top of the memory requested by the function per on a per instance basis\n  percentMemoryPadding: 10\n\n")),(0,r.kt)("p",null,"If you run functions worker embedded in a broker on Kubernetes, you can use the default settings. "),(0,r.kt)("h3",{id:"run-standalone-functions-worker-on-kubernetes"},"Run standalone functions worker on Kubernetes"),(0,r.kt)("p",null,"If you run functions worker standalone (that is, not embedded) on Kubernetes, you need to configure ",(0,r.kt)("inlineCode",{parentName:"p"},"pulsarSerivceUrl")," to be the URL of the broker and ",(0,r.kt)("inlineCode",{parentName:"p"},"pulsarAdminUrl")," as the URL to the functions worker."),(0,r.kt)("p",null,"For example, both Pulsar brokers and Function Workers run in the ",(0,r.kt)("inlineCode",{parentName:"p"},"pulsar")," K8S namespace. The brokers have a service called ",(0,r.kt)("inlineCode",{parentName:"p"},"brokers")," and the functions worker has a service called ",(0,r.kt)("inlineCode",{parentName:"p"},"func-worker"),". The settings are as follows:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},"\npulsarServiceUrl: pulsar://broker.pulsar:6650 // or pulsar+ssl://broker.pulsar:6651 if using TLS\npulsarAdminUrl: http://func-worker.pulsar:8080 // or https://func-worker:8443 if using TLS\n\n")),(0,r.kt)("h3",{id:"run-rbac-in-kubernetes-clusters"},"Run RBAC in Kubernetes clusters"),(0,r.kt)("p",null,"If you run RBAC in your Kubernetes cluster, make sure that the service account you use for running functions workers (or brokers, if functions workers run along with brokers) have permissions on the following Kubernetes APIs."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"services"),(0,r.kt)("li",{parentName:"ul"},"configmaps"),(0,r.kt)("li",{parentName:"ul"},"pods"),(0,r.kt)("li",{parentName:"ul"},"apps.statefulsets")),(0,r.kt)("p",null,"The following is sufficient:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},"\napiVersion: rbac.authorization.k8s.io/v1beta1\nkind: ClusterRole\nmetadata:\n  name: functions-worker\nrules:\n- apiGroups: [\"\"]\n  resources:\n  - services\n  - configmaps\n  - pods\n  verbs:\n  - '*'\n- apiGroups:\n  - apps\n  resources:\n  - statefulsets\n  verbs:\n  - '*'\n---\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: functions-worker\n---\napiVersion: rbac.authorization.k8s.io/v1beta1\nkind: ClusterRoleBinding\nmetadata:\n  name: functions-worker\nroleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: ClusterRole\n  name: functions-worker\nsubjectsKubernetesSec:\n- kind: ServiceAccount\n  name: functions-worker\n\n")),(0,r.kt)("p",null,"If the service-account is not properly configured, an error message similar to this is displayed:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},"\n22:04:27.696 [Timer-0] ERROR org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory - Error while trying to fetch configmap example-pulsar-4qvmb5gur3c6fc9dih0x1xn8b-function-worker-config at namespace pulsar\nio.kubernetes.client.ApiException: Forbidden\n    at io.kubernetes.client.ApiClient.handleResponse(ApiClient.java:882) ~[io.kubernetes-client-java-2.0.0.jar:?]\n    at io.kubernetes.client.ApiClient.execute(ApiClient.java:798) ~[io.kubernetes-client-java-2.0.0.jar:?]\n    at io.kubernetes.client.apis.CoreV1Api.readNamespacedConfigMapWithHttpInfo(CoreV1Api.java:23673) ~[io.kubernetes-client-java-api-2.0.0.jar:?]\n    at io.kubernetes.client.apis.CoreV1Api.readNamespacedConfigMap(CoreV1Api.java:23655) ~[io.kubernetes-client-java-api-2.0.0.jar:?]\n    at org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory.fetchConfigMap(KubernetesRuntimeFactory.java:284) [org.apache.pulsar-pulsar-functions-runtime-2.4.0-42c3bf949.jar:2.4.0-42c3bf949]\n    at org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory$1.run(KubernetesRuntimeFactory.java:275) [org.apache.pulsar-pulsar-functions-runtime-2.4.0-42c3bf949.jar:2.4.0-42c3bf949]\n    at java.util.TimerThread.mainLoop(Timer.java:555) [?:1.8.0_212]\n    at java.util.TimerThread.run(Timer.java:505) [?:1.8.0_212]\n\n")),(0,r.kt)("h3",{id:"integrate-kubernetes-secrets"},"Integrate Kubernetes secrets"),(0,r.kt)("p",null,"In order to safely distribute secrets, Pulasr Functions can reference Kubernetes secrets. To enable this, set the ",(0,r.kt)("inlineCode",{parentName:"p"},"secretsProviderConfiguratorClassName")," to ",(0,r.kt)("inlineCode",{parentName:"p"},"org.apache.pulsar.functions.secretsproviderconfigurator.KubernetesSecretsProviderConfigurator"),"."),(0,r.kt)("p",null,"You can create a secret in the namespace where your functions are deployed. For example, you deploy functions to the ",(0,r.kt)("inlineCode",{parentName:"p"},"pulsar-func")," Kubernetes namespace, and you have a secret named ",(0,r.kt)("inlineCode",{parentName:"p"},"database-creds")," with a field name ",(0,r.kt)("inlineCode",{parentName:"p"},"password"),", which you want to mount in the pod as an environment variable called ",(0,r.kt)("inlineCode",{parentName:"p"},"DATABASE_PASSWORD"),". The following functions configuration enables you to reference that secret and mount the value as an environment variable in the pod."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Yaml"},'\ntenant: "mytenant"\nnamespace: "mynamespace"\nname: "myfunction"\ntopicName: "persistent://mytenant/mynamespace/myfuncinput"\nclassName: "com.company.pulsar.myfunction"\n\nsecrets:\n  # the secret will be mounted from the `password` field in the `database-creds` secret as an env var called `DATABASE_PASSWORD`\n  DATABASE_PASSWORD:\n    path: "database-creds"\n    key: "password"\n\n')),(0,r.kt)("h3",{id:"enable-token-authentication"},"Enable token authentication"),(0,r.kt)("p",null,"When you enable authentication for your Pulsar cluster, you need a mechanism for the pod running your function to authenticate with the broker."),(0,r.kt)("p",null,"The ",(0,r.kt)("inlineCode",{parentName:"p"},"org.apache.pulsar.functions.auth.KubernetesFunctionAuthProvider")," interface provides support for any authentication mechanism. The ",(0,r.kt)("inlineCode",{parentName:"p"},"functionAuthProviderClassName")," in ",(0,r.kt)("inlineCode",{parentName:"p"},"function-worker.yml")," is used to specify your path to this implementation. "),(0,r.kt)("p",null,"Pulsar includes an implementation of this interface for token authentication, and distributes the certificate authority via the same implementation. The configuration is similar as follows:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Yaml"},"\nfunctionAuthProviderClassName: org.apache.pulsar.functions.auth.KubernetesSecretsTokenAuthProvider\n\n")),(0,r.kt)("p",null,"For token authentication, the functions worker captures the token that is used to deploy (or update) the function. The token is saved as a secret and mounted into the pod."),(0,r.kt)("p",null,"For custom authentication or TLS, you need to implement this interface or use an alternative mechanism to provide authentication. If you use token authentication and TLS encryption to secure the communication with the cluster, Pulsar passes your certificate authority (CA) to the client, so the client obtains what it needs to authenticate the cluster, and trusts the cluster with your signed certificate."),(0,r.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"If you use tokens that expire when deploying functions, these tokens will expire."))),(0,r.kt)("h3",{id:"run-clusters-with-authentication"},"Run clusters with authentication"),(0,r.kt)("p",null,"When you run a functions worker in a standalone process (that is, not embedded in the broker) in a cluster with authentication, you must configure your functions worker to interact with the broker and authenticate incoming requests. So you need to configure properties that the broker requires for authentication or authorization."),(0,r.kt)("p",null,"For example, if you use token authentication, you need to configure the following properties in the ",(0,r.kt)("inlineCode",{parentName:"p"},"function-worker.yml")," file."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Yaml"},'\nclientAuthenticationPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken\nclientAuthenticationParameters: file:///etc/pulsar/token/admin-token.txt\nconfigurationStoreServers: zookeeper-cluster:2181 # auth requires a connection to zookeeper\nauthenticationProviders:\n - "org.apache.pulsar.broker.authentication.AuthenticationProviderToken"\nauthorizationEnabled: true\nauthenticationEnabled: true\nsuperUserRoles:\n  - superuser\n  - proxy\nproperties:\n  tokenSecretKey: file:///etc/pulsar/jwt/secret # if using a secret token\n  tokenPublicKey: file:///etc/pulsar/jwt/public.key # if using public/private key tokens\n\n')),(0,r.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"You must configure both the Function Worker authorization or authentication for the server to authenticate requests and configure the client to be authenticated to communicate with the broker."))),(0,r.kt)("h3",{id:"customize-kubernetes-runtime"},"Customize Kubernetes runtime"),(0,r.kt)("p",null,"The Kubernetes integration enables you to implement a class and customize how to generate manifests. You can configure it by setting ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerClassName")," in the ",(0,r.kt)("inlineCode",{parentName:"p"},"functions-worker.yml")," file and use the fully qualified class name. You must implement the ",(0,r.kt)("inlineCode",{parentName:"p"},"org.apache.pulsar.functions.runtime.kubernetes.KubernetesManifestCustomizer")," interface."),(0,r.kt)("p",null,"The functions (and sinks/sources) API provides a flag, ",(0,r.kt)("inlineCode",{parentName:"p"},"customRuntimeOptions"),", which is passed to this interface."),(0,r.kt)("p",null,"To initialize the ",(0,r.kt)("inlineCode",{parentName:"p"},"KubernetesManifestCustomizer"),", you can provide ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerConfig")," in the ",(0,r.kt)("inlineCode",{parentName:"p"},"functions-worker.yml")," file. ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerConfig")," is passed to the ",(0,r.kt)("inlineCode",{parentName:"p"},"public void initialize(Map<String, Object> config)")," function of the interface. ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerConfig"),"is different from the ",(0,r.kt)("inlineCode",{parentName:"p"},"customRuntimeOptions")," as ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerConfig")," is the same across all functions. If you provide both ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerConfig"),"  and ",(0,r.kt)("inlineCode",{parentName:"p"},"customRuntimeOptions"),", you need to decide how to manage these two configurations in your implementation of ",(0,r.kt)("inlineCode",{parentName:"p"},"KubernetesManifestCustomizer"),". "),(0,r.kt)("p",null,"Pulsar includes a built-in implementation. To use the basic implementation, set ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerClassName")," to ",(0,r.kt)("inlineCode",{parentName:"p"},"org.apache.pulsar.functions.runtime.kubernetes.BasicKubernetesManifestCustomizer"),". The built-in implementation initialized with ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerConfig")," enables you to pass a JSON document as ",(0,r.kt)("inlineCode",{parentName:"p"},"customRuntimeOptions")," with certain properties to augment, which decides how the manifests are generated. If both ",(0,r.kt)("inlineCode",{parentName:"p"},"runtimeCustomizerConfig")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"customRuntimeOptions")," are provided, ",(0,r.kt)("inlineCode",{parentName:"p"},"BasicKubernetesManifestCustomizer")," uses ",(0,r.kt)("inlineCode",{parentName:"p"},"customRuntimeOptions")," to override the configuration if there are conflicts in these two configurations. "),(0,r.kt)("p",null,"Below is an example of ",(0,r.kt)("inlineCode",{parentName:"p"},"customRuntimeOptions"),"."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-json"},'\n{\n  "jobName": "jobname", // the k8s pod name to run this function instance\n  "jobNamespace": "namespace", // the k8s namespace to run this function in\n  "extractLabels": {           // extra labels to attach to the statefulSet, service, and pods\n    "extraLabel": "value"\n  },\n  "extraAnnotations": {        // extra annotations to attach to the statefulSet, service, and pods\n    "extraAnnotation": "value"\n  },\n  "nodeSelectorLabels": {      // node selector labels to add on to the pod spec\n    "customLabel": "value"\n  },\n  "tolerations": [             // tolerations to add to the pod spec\n    {\n      "key": "custom-key",\n      "value": "value",\n      "effect": "NoSchedule"\n    }\n  ],\n  "resourceRequirements": {  // values for cpu and memory should be defined as described here: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container\n    "requests": {\n      "cpu": 1,\n      "memory": "4G"\n    },\n    "limits": {\n      "cpu": 2,\n      "memory": "8G"\n    }\n  }\n}\n\n')),(0,r.kt)("h2",{id:"run-clusters-with-geo-replication"},"Run clusters with geo-replication"),(0,r.kt)("p",null,"If you run multiple clusters tied together with geo-replication, it is important to use a different function namespace for each cluster. Otherwise, the function shares a namespace and potentially schedule across clusters."),(0,r.kt)("p",null,"For example, if you have two clusters: ",(0,r.kt)("inlineCode",{parentName:"p"},"east-1")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"west-1"),", you can configure the functions workers for ",(0,r.kt)("inlineCode",{parentName:"p"},"east-1")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"west-1")," perspectively as follows."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Yaml"},"\npulsarFunctionsCluster: east-1\npulsarFunctionsNamespace: public/functions-east-1\n\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Yaml"},"\npulsarFunctionsCluster: west-1\npulsarFunctionsNamespace: public/functions-west-1\n\n")),(0,r.kt)("p",null,"This ensures the two different Functions Workers use distinct sets of topics for their internal coordination."),(0,r.kt)("h2",{id:"configure-standalone-functions-worker"},"Configure standalone functions worker"),(0,r.kt)("p",null,"When configuring a standalone functions worker, you need to configure properties that the broker requires, especially if you use TLS. And then Functions Worker can communicate with the broker. "),(0,r.kt)("p",null,"You need to configure the following required properties."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Yaml"},"\nworkerPort: 8080\nworkerPortTls: 8443 # when using TLS\ntlsCertificateFilePath: /etc/pulsar/tls/tls.crt # when using TLS\ntlsKeyFilePath: /etc/pulsar/tls/tls.key # when using TLS\ntlsTrustCertsFilePath: /etc/pulsar/tls/ca.crt # when using TLS\npulsarServiceUrl: pulsar://broker.pulsar:6650/ # or pulsar+ssl://pulsar-prod-broker.pulsar:6651/ when using TLS\npulsarWebServiceUrl: http://broker.pulsar:8080/ # or https://pulsar-prod-broker.pulsar:8443/ when using TLS\nuseTls: true # when using TLS, critical!\n\n")))}m.isMDXComponent=!0}}]);
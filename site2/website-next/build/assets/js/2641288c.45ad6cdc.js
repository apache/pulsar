"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[56832],{3905:function(e,a,t){t.d(a,{Zo:function(){return u},kt:function(){return d}});var n=t(67294);function r(e,a,t){return a in e?Object.defineProperty(e,a,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[a]=t,e}function l(e,a){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);a&&(n=n.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),t.push.apply(t,n)}return t}function p(e){for(var a=1;a<arguments.length;a++){var t=null!=arguments[a]?arguments[a]:{};a%2?l(Object(t),!0).forEach((function(a){r(e,a,t[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):l(Object(t)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(t,a))}))}return e}function s(e,a){if(null==e)return{};var t,n,r=function(e,a){if(null==e)return{};var t,n,r={},l=Object.keys(e);for(n=0;n<l.length;n++)t=l[n],a.indexOf(t)>=0||(r[t]=e[t]);return r}(e,a);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)t=l[n],a.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var i=n.createContext({}),o=function(e){var a=n.useContext(i),t=a;return e&&(t="function"==typeof e?e(a):p(p({},a),e)),t},u=function(e){var a=o(e.components);return n.createElement(i.Provider,{value:a},e.children)},m={inlineCode:"code",wrapper:function(e){var a=e.children;return n.createElement(n.Fragment,{},a)}},c=n.forwardRef((function(e,a){var t=e.components,r=e.mdxType,l=e.originalType,i=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),c=o(t),d=r,k=c["".concat(i,".").concat(d)]||c[d]||m[d]||l;return t?n.createElement(k,p(p({ref:a},u),{},{components:t})):n.createElement(k,p({ref:a},u))}));function d(e,a){var t=arguments,r=a&&a.mdxType;if("string"==typeof e||r){var l=t.length,p=new Array(l);p[0]=c;var s={};for(var i in a)hasOwnProperty.call(a,i)&&(s[i]=a[i]);s.originalType=e,s.mdxType="string"==typeof e?e:r,p[1]=s;for(var o=2;o<l;o++)p[o]=t[o];return n.createElement.apply(null,p)}return n.createElement.apply(null,t)}c.displayName="MDXCreateElement"},85160:function(e,a,t){t.r(a),t.d(a,{frontMatter:function(){return s},contentTitle:function(){return i},metadata:function(){return o},toc:function(){return u},default:function(){return c}});var n=t(87462),r=t(63366),l=(t(67294),t(3905)),p=["components"],s={id:"kubernetes-helm",title:"Get started in Kubernetes",sidebar_label:"Run Pulsar in Kubernetes",original_id:"kubernetes-helm"},i=void 0,o={unversionedId:"kubernetes-helm",id:"version-2.7.3/kubernetes-helm",isDocsHomePage:!1,title:"Get started in Kubernetes",description:"This section guides you through every step of installing and running Apache Pulsar with Helm on Kubernetes quickly, including the following sections:",source:"@site/versioned_docs/version-2.7.3/getting-started-helm.md",sourceDirName:".",slug:"/kubernetes-helm",permalink:"/docs/2.7.3/kubernetes-helm",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.3/getting-started-helm.md",tags:[],version:"2.7.3",frontMatter:{id:"kubernetes-helm",title:"Get started in Kubernetes",sidebar_label:"Run Pulsar in Kubernetes",original_id:"kubernetes-helm"},sidebar:"version-2.7.3/docsSidebar",previous:{title:"Run Pulsar in Docker",permalink:"/docs/2.7.3/standalone-docker"},next:{title:"Overview",permalink:"/docs/2.7.3/concepts-overview"}},u=[{value:"Prerequisite",id:"prerequisite",children:[]},{value:"Step 0: Prepare a Kubernetes cluster",id:"step-0-prepare-a-kubernetes-cluster",children:[]},{value:"Step 1: Install Pulsar Helm chart",id:"step-1-install-pulsar-helm-chart",children:[]},{value:"Step 2: Use pulsar-admin to create Pulsar tenants/namespaces/topics",id:"step-2-use-pulsar-admin-to-create-pulsar-tenantsnamespacestopics",children:[]},{value:"Step 3: Use Pulsar client to produce and consume messages",id:"step-3-use-pulsar-client-to-produce-and-consume-messages",children:[]},{value:"Step 4: Use Pulsar Manager to manage the cluster",id:"step-4-use-pulsar-manager-to-manage-the-cluster",children:[]},{value:"Step 5: Use Prometheus and Grafana to monitor cluster",id:"step-5-use-prometheus-and-grafana-to-monitor-cluster",children:[]}],m={toc:u};function c(e){var a=e.components,t=(0,r.Z)(e,p);return(0,l.kt)("wrapper",(0,n.Z)({},m,t,{components:a,mdxType:"MDXLayout"}),(0,l.kt)("p",null,"This section guides you through every step of installing and running Apache Pulsar with Helm on Kubernetes quickly, including the following sections:"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Install the Apache Pulsar on Kubernetes using Helm"),(0,l.kt)("li",{parentName:"ul"},"Start and stop Apache Pulsar"),(0,l.kt)("li",{parentName:"ul"},"Create topics using ",(0,l.kt)("inlineCode",{parentName:"li"},"pulsar-admin")),(0,l.kt)("li",{parentName:"ul"},"Produce and consume messages using Pulsar clients"),(0,l.kt)("li",{parentName:"ul"},"Monitor Apache Pulsar status with Prometheus and Grafana")),(0,l.kt)("p",null,"For deploying a Pulsar cluster for production usage, read the documentation on ",(0,l.kt)("a",{parentName:"p",href:"helm-deploy"},"how to configure and install a Pulsar Helm chart"),"."),(0,l.kt)("h2",{id:"prerequisite"},"Prerequisite"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Kubernetes server 1.14.0+"),(0,l.kt)("li",{parentName:"ul"},"kubectl 1.14.0+"),(0,l.kt)("li",{parentName:"ul"},"Helm 3.0+")),(0,l.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,l.kt)("div",{parentName:"div",className:"admonition-heading"},(0,l.kt)("h5",{parentName:"div"},(0,l.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,l.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,l.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,l.kt)("div",{parentName:"div",className:"admonition-content"},(0,l.kt)("p",{parentName:"div"},"For the following steps, step 2 and step 3 are for ",(0,l.kt)("strong",{parentName:"p"},"developers")," and step 4 and step 5 are for ",(0,l.kt)("strong",{parentName:"p"},"administrators"),"."))),(0,l.kt)("h2",{id:"step-0-prepare-a-kubernetes-cluster"},"Step 0: Prepare a Kubernetes cluster"),(0,l.kt)("p",null,"Before installing a Pulsar Helm chart, you have to create a Kubernetes cluster. You can follow ",(0,l.kt)("a",{parentName:"p",href:"helm-prepare"},"the instructions")," to prepare a Kubernetes cluster."),(0,l.kt)("p",null,"We use ",(0,l.kt)("a",{parentName:"p",href:"https://minikube.sigs.k8s.io/docs/start/"},"Minikube")," in this quick start guide. To prepare a Kubernetes cluster, follow these steps:"),(0,l.kt)("ol",null,(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Create a Kubernetes cluster on Minikube."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nminikube start --memory=8192 --cpus=4 --kubernetes-version=<k8s-version>\n\n")),(0,l.kt)("p",{parentName:"li"},"The ",(0,l.kt)("inlineCode",{parentName:"p"},"<k8s-version>")," can be any ",(0,l.kt)("a",{parentName:"p",href:"https://minikube.sigs.k8s.io/docs/reference/configuration/kubernetes/"},"Kubernetes version supported by your Minikube installation"),", such as ",(0,l.kt)("inlineCode",{parentName:"p"},"v1.16.1"),".")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Set ",(0,l.kt)("inlineCode",{parentName:"p"},"kubectl")," to use Minikube."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nkubectl config use-context minikube\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"To use the ",(0,l.kt)("a",{parentName:"p",href:"https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/"},"Kubernetes Dashboard")," with the local Kubernetes cluster on Minikube, enter the command below:"),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nminikube dashboard\n\n")),(0,l.kt)("p",{parentName:"li"},"The command automatically triggers opening a webpage in your browser. "))),(0,l.kt)("h2",{id:"step-1-install-pulsar-helm-chart"},"Step 1: Install Pulsar Helm chart"),(0,l.kt)("ol",{start:0},(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Add Pulsar charts repo."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nhelm repo add apache https://pulsar.apache.org/charts\n\n")),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nhelm repo update\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Clone the Pulsar Helm chart repository."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\ngit clone https://github.com/apache/pulsar-helm-chart\ncd pulsar-helm-chart\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Run the script ",(0,l.kt)("inlineCode",{parentName:"p"},"prepare_helm_release.sh")," to create secrets required for installing the Apache Pulsar Helm chart. The username ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar")," and password ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar")," are used for logging into the Grafana dashboard and Pulsar Manager."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\n./scripts/pulsar/prepare_helm_release.sh \\\n    -n pulsar \\\n    -k pulsar-mini \\\n    -c\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Use the Pulsar Helm chart to install a Pulsar cluster to Kubernetes."),(0,l.kt)("blockquote",{parentName:"li"},(0,l.kt)("p",{parentName:"blockquote"},(0,l.kt)("strong",{parentName:"p"},"NOTE"),(0,l.kt)("br",{parentName:"p"}),"\n","You need to specify ",(0,l.kt)("inlineCode",{parentName:"p"},"--set initialize=true")," when installing Pulsar the first time. This command installs and starts Apache Pulsar.")),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nhelm install \\\n    --values examples/values-minikube.yaml \\\n    --set initialize=true \\\n    --namespace pulsar \\\n    pulsar-mini apache/pulsar\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Check the status of all pods."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nkubectl get pods -n pulsar\n\n")),(0,l.kt)("p",{parentName:"li"},"If all pods start up successfully, you can see that the ",(0,l.kt)("inlineCode",{parentName:"p"},"STATUS")," is changed to ",(0,l.kt)("inlineCode",{parentName:"p"},"Running")," or ",(0,l.kt)("inlineCode",{parentName:"p"},"Completed"),"."),(0,l.kt)("p",{parentName:"li"},(0,l.kt)("strong",{parentName:"p"},"Output")),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nNAME                                         READY   STATUS      RESTARTS   AGE\npulsar-mini-bookie-0                         1/1     Running     0          9m27s\npulsar-mini-bookie-init-5gphs                0/1     Completed   0          9m27s\npulsar-mini-broker-0                         1/1     Running     0          9m27s\npulsar-mini-grafana-6b7bcc64c7-4tkxd         1/1     Running     0          9m27s\npulsar-mini-prometheus-5fcf5dd84c-w8mgz      1/1     Running     0          9m27s\npulsar-mini-proxy-0                          1/1     Running     0          9m27s\npulsar-mini-pulsar-init-t7cqt                0/1     Completed   0          9m27s\npulsar-mini-pulsar-manager-9bcbb4d9f-htpcs   1/1     Running     0          9m27s\npulsar-mini-toolset-0                        1/1     Running     0          9m27s\npulsar-mini-zookeeper-0                      1/1     Running     0          9m27s\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Check the status of all services in the namespace ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar"),"."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nkubectl get services -n pulsar\n\n")),(0,l.kt)("p",{parentName:"li"},(0,l.kt)("strong",{parentName:"p"},"Output")),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nNAME                         TYPE           CLUSTER-IP       EXTERNAL-IP   PORT(S)                       AGE\npulsar-mini-bookie           ClusterIP      None             <none>        3181/TCP,8000/TCP             11m\npulsar-mini-broker           ClusterIP      None             <none>        8080/TCP,6650/TCP             11m\npulsar-mini-grafana          LoadBalancer   10.106.141.246   <pending>     3000:31905/TCP                11m\npulsar-mini-prometheus       ClusterIP      None             <none>        9090/TCP                      11m\npulsar-mini-proxy            LoadBalancer   10.97.240.109    <pending>     80:32305/TCP,6650:31816/TCP   11m\npulsar-mini-pulsar-manager   LoadBalancer   10.103.192.175   <pending>     9527:30190/TCP                11m\npulsar-mini-toolset          ClusterIP      None             <none>        <none>                        11m\npulsar-mini-zookeeper        ClusterIP      None             <none>        2888/TCP,3888/TCP,2181/TCP    11m\n\n")))),(0,l.kt)("h2",{id:"step-2-use-pulsar-admin-to-create-pulsar-tenantsnamespacestopics"},"Step 2: Use pulsar-admin to create Pulsar tenants/namespaces/topics"),(0,l.kt)("p",null,(0,l.kt)("inlineCode",{parentName:"p"},"pulsar-admin")," is the CLI (command-Line Interface) tool for Pulsar. In this step, you can use ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar-admin")," to create resources, including tenants, namespaces, and topics."),(0,l.kt)("ol",null,(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Enter the ",(0,l.kt)("inlineCode",{parentName:"p"},"toolset")," container."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nkubectl exec -it -n pulsar pulsar-mini-toolset-0 -- /bin/bash\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"In the ",(0,l.kt)("inlineCode",{parentName:"p"},"toolset")," container, create a tenant named ",(0,l.kt)("inlineCode",{parentName:"p"},"apache"),"."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin tenants create apache\n\n")),(0,l.kt)("p",{parentName:"li"},"Then you can list the tenants to see if the tenant is created successfully."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin tenants list\n\n")),(0,l.kt)("p",{parentName:"li"},"You should see a similar output as below. The tenant ",(0,l.kt)("inlineCode",{parentName:"p"},"apache")," has been successfully created. "),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},'\n"apache"\n"public"\n"pulsar"\n\n'))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"In the ",(0,l.kt)("inlineCode",{parentName:"p"},"toolset")," container, create a namespace named ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar")," in the tenant ",(0,l.kt)("inlineCode",{parentName:"p"},"apache"),"."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin namespaces create apache/pulsar\n\n")),(0,l.kt)("p",{parentName:"li"},"Then you can list the namespaces of tenant ",(0,l.kt)("inlineCode",{parentName:"p"},"apache")," to see if the namespace is created successfully."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin namespaces list apache\n\n")),(0,l.kt)("p",{parentName:"li"},"You should see a similar output as below. The namespace ",(0,l.kt)("inlineCode",{parentName:"p"},"apache/pulsar")," has been successfully created. "),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},'\n"apache/pulsar"\n\n'))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"In the ",(0,l.kt)("inlineCode",{parentName:"p"},"toolset")," container, create a topic ",(0,l.kt)("inlineCode",{parentName:"p"},"test-topic")," with ",(0,l.kt)("inlineCode",{parentName:"p"},"4")," partitions in the namespace ",(0,l.kt)("inlineCode",{parentName:"p"},"apache/pulsar"),"."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin topics create-partitioned-topic apache/pulsar/test-topic -p 4\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"In the ",(0,l.kt)("inlineCode",{parentName:"p"},"toolset")," container, list all the partitioned topics in the namespace ",(0,l.kt)("inlineCode",{parentName:"p"},"apache/pulsar"),"."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin topics list-partitioned-topics apache/pulsar\n\n")),(0,l.kt)("p",{parentName:"li"},"Then you can see all the partitioned topics in the namespace ",(0,l.kt)("inlineCode",{parentName:"p"},"apache/pulsar"),"."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},'\n"persistent://apache/pulsar/test-topic"\n\n')))),(0,l.kt)("h2",{id:"step-3-use-pulsar-client-to-produce-and-consume-messages"},"Step 3: Use Pulsar client to produce and consume messages"),(0,l.kt)("p",null,"You can use the Pulsar client to create producers and consumers to produce and consume messages."),(0,l.kt)("p",null,"By default, the Pulsar Helm chart exposes the Pulsar cluster through a Kubernetes ",(0,l.kt)("inlineCode",{parentName:"p"},"LoadBalancer"),". In Minikube, you can use the following command to check the proxy service."),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nkubectl get services -n pulsar | grep pulsar-mini-proxy\n\n")),(0,l.kt)("p",null,"You will see a similar output as below."),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\npulsar-mini-proxy            LoadBalancer   10.97.240.109    <pending>     80:32305/TCP,6650:31816/TCP   28m\n\n")),(0,l.kt)("p",null,"This output tells what are the node ports that Pulsar cluster's binary port and HTTP port are mapped to. The port after ",(0,l.kt)("inlineCode",{parentName:"p"},"80:")," is the HTTP port while the port after ",(0,l.kt)("inlineCode",{parentName:"p"},"6650:")," is the binary port."),(0,l.kt)("p",null,"Then you can find the IP address and exposed ports of your Minikube server by running the following command."),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nminikube service pulsar-mini-proxy -n pulsar\n\n")),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Output")),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\n|-----------|-------------------|-------------|-------------------------|\n| NAMESPACE |       NAME        | TARGET PORT |           URL           |\n|-----------|-------------------|-------------|-------------------------|\n| pulsar    | pulsar-mini-proxy | http/80     | http://172.17.0.4:32305 |\n|           |                   | pulsar/6650 | http://172.17.0.4:31816 |\n|-----------|-------------------|-------------|-------------------------|\n\ud83c\udfc3  Starting tunnel for service pulsar-mini-proxy.\n|-----------|-------------------|-------------|------------------------|\n| NAMESPACE |       NAME        | TARGET PORT |          URL           |\n|-----------|-------------------|-------------|------------------------|\n| pulsar    | pulsar-mini-proxy |             | http://127.0.0.1:61853 |\n|           |                   |             | http://127.0.0.1:61854 |\n|-----------|-------------------|-------------|------------------------|\n\n")),(0,l.kt)("p",null,"At this point, you can get the service URLs to connect to your Pulsar client. Here are URL examples:"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre"},"\nwebServiceUrl=http://127.0.0.1:61853/\nbrokerServiceUrl=pulsar://127.0.0.1:61854/\n\n")),(0,l.kt)("p",null,"Then you can proceed with the following steps:"),(0,l.kt)("ol",null,(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Download the Apache Pulsar tarball from the ",(0,l.kt)("a",{parentName:"p",href:"https://pulsar.apache.org/en/download/"},"downloads page"),".")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Decompress the tarball based on your download file."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\ntar -xf <file-name>.tar.gz\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Expose ",(0,l.kt)("inlineCode",{parentName:"p"},"PULSAR_HOME"),"."),(0,l.kt)("p",{parentName:"li"},"(1) Enter the directory of the decompressed download file."),(0,l.kt)("p",{parentName:"li"},"(2) Expose ",(0,l.kt)("inlineCode",{parentName:"p"},"PULSAR_HOME")," as the environment variable."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nexport PULSAR_HOME=$(pwd)\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Configure the Pulsar client."),(0,l.kt)("p",{parentName:"li"},"In the ",(0,l.kt)("inlineCode",{parentName:"p"},"${PULSAR_HOME}/conf/client.conf")," file, replace ",(0,l.kt)("inlineCode",{parentName:"p"},"webServiceUrl")," and ",(0,l.kt)("inlineCode",{parentName:"p"},"brokerServiceUrl")," with the service URLs you get from the above steps.")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Create a subscription to consume messages from ",(0,l.kt)("inlineCode",{parentName:"p"},"apache/pulsar/test-topic"),"."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-client consume -s sub apache/pulsar/test-topic  -n 0\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Open a new terminal. In the new terminal, create a producer and send 10 messages to the ",(0,l.kt)("inlineCode",{parentName:"p"},"test-topic")," topic."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},'\nbin/pulsar-client produce apache/pulsar/test-topic  -m "---------hello apache pulsar-------" -n 10\n\n'))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"Verify the results."),(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"From the producer side"),(0,l.kt)("p",{parentName:"li"},"  ",(0,l.kt)("strong",{parentName:"p"},"Output")),(0,l.kt)("p",{parentName:"li"},"  The messages have been produced successfully."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\n18:15:15.489 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 10 messages successfully produced\n\n"))),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"From the consumer side"),(0,l.kt)("p",{parentName:"li"},"  ",(0,l.kt)("strong",{parentName:"p"},"Output")),(0,l.kt)("p",{parentName:"li"},"  At the same time, you can receive the messages as below."),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n----- got message -----\n---------hello apache pulsar-------\n\n")))))),(0,l.kt)("h2",{id:"step-4-use-pulsar-manager-to-manage-the-cluster"},"Step 4: Use Pulsar Manager to manage the cluster"),(0,l.kt)("p",null,(0,l.kt)("a",{parentName:"p",href:"administration-pulsar-manager"},"Pulsar Manager")," is a web-based GUI management tool for managing and monitoring Pulsar."),(0,l.kt)("ol",null,(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"By default, the ",(0,l.kt)("inlineCode",{parentName:"p"},"Pulsar Manager")," is exposed as a separate ",(0,l.kt)("inlineCode",{parentName:"p"},"LoadBalancer"),". You can open the Pulsar Manager UI using the following command:"),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nminikube service -n pulsar pulsar-mini-pulsar-manager\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"The Pulsar Manager UI will be open in your browser. You can use the username ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar")," and password ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar")," to log into Pulsar Manager.")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"In Pulsar Manager UI, you can create an environment. "),(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},"Click ",(0,l.kt)("inlineCode",{parentName:"li"},"New Environment")," button in the top-left corner."),(0,l.kt)("li",{parentName:"ul"},"Type ",(0,l.kt)("inlineCode",{parentName:"li"},"pulsar-mini")," for the field ",(0,l.kt)("inlineCode",{parentName:"li"},"Environment Name")," in the popup window."),(0,l.kt)("li",{parentName:"ul"},"Type ",(0,l.kt)("inlineCode",{parentName:"li"},"http://pulsar-mini-broker:8080")," for the field ",(0,l.kt)("inlineCode",{parentName:"li"},"Service URL")," in the popup window."),(0,l.kt)("li",{parentName:"ul"},"Click ",(0,l.kt)("inlineCode",{parentName:"li"},"Confirm")," button in the popup window."))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"After successfully created an environment, you are redirected to the ",(0,l.kt)("inlineCode",{parentName:"p"},"tenants")," page of that environment. Then you can create ",(0,l.kt)("inlineCode",{parentName:"p"},"tenants"),", ",(0,l.kt)("inlineCode",{parentName:"p"},"namespaces")," and ",(0,l.kt)("inlineCode",{parentName:"p"},"topics")," using the Pulsar Manager."))),(0,l.kt)("h2",{id:"step-5-use-prometheus-and-grafana-to-monitor-cluster"},"Step 5: Use Prometheus and Grafana to monitor cluster"),(0,l.kt)("p",null,"Grafana is an open-source visualization tool, which can be used for visualizing time series data into dashboards."),(0,l.kt)("ol",null,(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"By default, the Grafana is exposed as a separate ",(0,l.kt)("inlineCode",{parentName:"p"},"LoadBalancer"),". You can open the Grafana UI using the following command:"),(0,l.kt)("pre",{parentName:"li"},(0,l.kt)("code",{parentName:"pre",className:"language-bash"},"\nminikube service pulsar-mini-grafana -n pulsar\n\n"))),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"The Grafana UI is open in your browser. You can use the username ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar")," and password ",(0,l.kt)("inlineCode",{parentName:"p"},"pulsar")," to log into the Grafana Dashboard.")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"You can view dashboards for different components of a Pulsar cluster."))))}c.isMDXComponent=!0}}]);
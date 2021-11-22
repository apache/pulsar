"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[4717],{3905:function(e,t,n){n.d(t,{Zo:function(){return c},kt:function(){return k}});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),u=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},c=function(e){var t=u(e.components);return r.createElement(s.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),d=u(n),k=a,h=d["".concat(s,".").concat(k)]||d[k]||p[k]||i;return n?r.createElement(h,o(o({ref:t},c),{},{components:n})):r.createElement(h,o({ref:t},c))}));function k(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,o=new Array(i);o[0]=d;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:a,o[1]=l;for(var u=2;u<i;u++)o[u]=n[u];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},97316:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return l},contentTitle:function(){return s},metadata:function(){return u},toc:function(){return c},default:function(){return d}});var r=n(87462),a=n(63366),i=(n(67294),n(3905)),o=["components"],l={id:"security-kerberos",title:"Authentication using Kerberos",sidebar_label:"Authentication using Kerberos"},s=void 0,u={unversionedId:"security-kerberos",id:"security-kerberos",isDocsHomePage:!1,title:"Authentication using Kerberos",description:"Kerberos is a network authentication protocol. By using secret-key cryptography, Kerberos is designed to provide strong authentication for client applications and server applications.",source:"@site/docs/security-kerberos.md",sourceDirName:".",slug:"/security-kerberos",permalink:"/docs/next/security-kerberos",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/docs/security-kerberos.md",tags:[],version:"current",frontMatter:{id:"security-kerberos",title:"Authentication using Kerberos",sidebar_label:"Authentication using Kerberos"},sidebar:"docsSidebar",previous:{title:"Authentication using Athenz",permalink:"/docs/next/security-athenz"},next:{title:"Authentication using OAuth 2.0 access tokens",permalink:"/docs/next/security-oauth2"}},c=[{value:"Configuration for Kerberos between Client and Broker",id:"configuration-for-kerberos-between-client-and-broker",children:[{value:"Prerequisites",id:"prerequisites",children:[]},{value:"Kerberos configuration for Brokers",id:"kerberos-configuration-for-brokers",children:[]},{value:"Kerberos configuration for clients",id:"kerberos-configuration-for-clients",children:[]}]},{value:"Kerberos configuration for working with Pulsar Proxy",id:"kerberos-configuration-for-working-with-pulsar-proxy",children:[{value:"Create principal for Pulsar Proxy in Kerberos",id:"create-principal-for-pulsar-proxy-in-kerberos",children:[]},{value:"Add a section in JAAS configuration file for Pulsar Proxy",id:"add-a-section-in-jaas-configuration-file-for-pulsar-proxy",children:[]},{value:"Proxy client configuration",id:"proxy-client-configuration",children:[]},{value:"Kerberos configuration for Pulsar proxy service",id:"kerberos-configuration-for-pulsar-proxy-service",children:[]},{value:"Broker side configuration.",id:"broker-side-configuration",children:[]}]},{value:"Regarding authorization and role token",id:"regarding-authorization-and-role-token",children:[]},{value:"Regarding authentication between ZooKeeper and Broker",id:"regarding-authentication-between-zookeeper-and-broker",children:[]},{value:"Regarding authentication between BookKeeper and Broker",id:"regarding-authentication-between-bookkeeper-and-broker",children:[]}],p={toc:c};function d(e){var t=e.components,n=(0,a.Z)(e,o);return(0,i.kt)("wrapper",(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://web.mit.edu/kerberos/"},"Kerberos")," is a network authentication protocol. By using secret-key cryptography, ",(0,i.kt)("a",{parentName:"p",href:"https://web.mit.edu/kerberos/"},"Kerberos")," is designed to provide strong authentication for client applications and server applications. "),(0,i.kt)("p",null,"In Pulsar, you can use Kerberos with ",(0,i.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer"},"SASL")," as a choice for authentication. And Pulsar uses the ",(0,i.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Java_Authentication_and_Authorization_Service"},"Java Authentication and Authorization Service (JAAS)")," for SASL configuration. You need to provide JAAS configurations for Kerberos authentication. "),(0,i.kt)("p",null,"This document introduces how to configure ",(0,i.kt)("inlineCode",{parentName:"p"},"Kerberos")," with ",(0,i.kt)("inlineCode",{parentName:"p"},"SASL")," between Pulsar clients and brokers and how to configure Kerberos for Pulsar proxy in detail."),(0,i.kt)("h2",{id:"configuration-for-kerberos-between-client-and-broker"},"Configuration for Kerberos between Client and Broker"),(0,i.kt)("h3",{id:"prerequisites"},"Prerequisites"),(0,i.kt)("p",null,"To begin, you need to set up (or already have) a ",(0,i.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Key_distribution_center"},"Key Distribution Center(KDC)"),". Also you need to configure and run the ",(0,i.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Key_distribution_center"},"Key Distribution Center(KDC)"),"in advance. "),(0,i.kt)("p",null,"If your organization already uses a Kerberos server (for example, by using ",(0,i.kt)("inlineCode",{parentName:"p"},"Active Directory"),"), you do not have to install a new server for Pulsar. If your organization does not use a Kerberos server, you need to install one. Your Linux vendor might have packages for ",(0,i.kt)("inlineCode",{parentName:"p"},"Kerberos"),". On how to install and configure Kerberos, refer to ",(0,i.kt)("a",{parentName:"p",href:"https://help.ubuntu.com/community/Kerberos"},"Ubuntu"),",\n",(0,i.kt)("a",{parentName:"p",href:"https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/installing-kerberos.html"},"Redhat"),"."),(0,i.kt)("p",null,"Note that if you use Oracle Java, you need to download JCE policy files for your Java version and copy them to the ",(0,i.kt)("inlineCode",{parentName:"p"},"$JAVA_HOME/jre/lib/security")," directory."),(0,i.kt)("h4",{id:"kerberos-principals"},"Kerberos principals"),(0,i.kt)("p",null,"If you use the existing Kerberos system, ask your Kerberos administrator for a principal for each Brokers in your cluster and for every operating system user that accesses Pulsar with Kerberos authentication(via clients and tools)."),(0,i.kt)("p",null,"If you have installed your own Kerberos system, you can create these principals with the following commands:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n### add Principals for broker\nsudo /usr/sbin/kadmin.local -q 'addprinc -randkey broker/{hostname}@{REALM}'\nsudo /usr/sbin/kadmin.local -q \"ktadd -k /etc/security/keytabs/{broker-keytabname}.keytab broker/{hostname}@{REALM}\"\n### add Principals for client\nsudo /usr/sbin/kadmin.local -q 'addprinc -randkey client/{hostname}@{REALM}'\nsudo /usr/sbin/kadmin.local -q \"ktadd -k /etc/security/keytabs/{client-keytabname}.keytab client/{hostname}@{REALM}\"\n\n")),(0,i.kt)("p",null,"Note that ",(0,i.kt)("em",{parentName:"p"},"Kerberos")," requires that all your hosts can be resolved with their FQDNs."),(0,i.kt)("p",null,"The first part of Broker principal (for example, ",(0,i.kt)("inlineCode",{parentName:"p"},"broker")," in ",(0,i.kt)("inlineCode",{parentName:"p"},"broker/{hostname}@{REALM}"),") is the ",(0,i.kt)("inlineCode",{parentName:"p"},"serverType")," of each host. The suggested values of ",(0,i.kt)("inlineCode",{parentName:"p"},"serverType")," are ",(0,i.kt)("inlineCode",{parentName:"p"},"broker")," (host machine runs service Pulsar Broker) and ",(0,i.kt)("inlineCode",{parentName:"p"},"proxy")," (host machine runs service Pulsar Proxy). "),(0,i.kt)("h4",{id:"configure-how-to-connect-to-kdc"},"Configure how to connect to KDC"),(0,i.kt)("p",null,"You need to enter the command below to specify the path to the ",(0,i.kt)("inlineCode",{parentName:"p"},"krb5.conf")," file for the client side and the broker side. The content of ",(0,i.kt)("inlineCode",{parentName:"p"},"krb5.conf")," file indicates the default Realm and KDC information. See ",(0,i.kt)("a",{parentName:"p",href:"https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html"},"JDK\u2019s Kerberos Requirements")," for more details."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n-Djava.security.krb5.conf=/etc/pulsar/krb5.conf\n\n")),(0,i.kt)("p",null,"Here is an example of the krb5.conf file:"),(0,i.kt)("p",null,"In the configuration file, ",(0,i.kt)("inlineCode",{parentName:"p"},"EXAMPLE.COM")," is the default realm; ",(0,i.kt)("inlineCode",{parentName:"p"},"kdc = localhost:62037")," is the kdc server url for realm ",(0,i.kt)("inlineCode",{parentName:"p"},"EXAMPLE.COM "),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\n[libdefaults]\n default_realm = EXAMPLE.COM\n\n[realms]\n EXAMPLE.COM  = {\n  kdc = localhost:62037\n }\n\n")),(0,i.kt)("p",null,"Usually machines configured with kerberos already have a system wide configuration and this configuration is optional."),(0,i.kt)("h4",{id:"jaas-configuration-file"},"JAAS configuration file"),(0,i.kt)("p",null,"You need JAAS configuration file for the client side and the broker side. JAAS configuration file provides the section of information that is used to connect KDC. Here is an example named ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'\n PulsarBroker {\n   com.sun.security.auth.module.Krb5LoginModule required\n   useKeyTab=true\n   storeKey=true\n   useTicketCache=false\n   keyTab="/etc/security/keytabs/pulsarbroker.keytab"\n   principal="broker/localhost@EXAMPLE.COM";\n};\n\n PulsarClient {\n   com.sun.security.auth.module.Krb5LoginModule required\n   useKeyTab=true\n   storeKey=true\n   useTicketCache=false\n   keyTab="/etc/security/keytabs/pulsarclient.keytab"\n   principal="client/localhost@EXAMPLE.COM";\n};\n\n')),(0,i.kt)("p",null,"You need to set the ",(0,i.kt)("inlineCode",{parentName:"p"},"JAAS")," configuration file path as JVM parameter for client and broker. For example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n    -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf\n\n")),(0,i.kt)("p",null,"In the ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf")," file above "),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("inlineCode",{parentName:"li"},"PulsarBroker")," is a section name in the JAAS file that each broker uses. This section tells the broker to use which principal inside Kerberos and the location of the keytab where the principal is stored. ",(0,i.kt)("inlineCode",{parentName:"li"},"PulsarBroker")," allows the broker to use the keytab specified in this section."),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("inlineCode",{parentName:"li"},"PulsarClient")," is a section name in the JASS file that each broker uses. This section tells the client to use which principal inside Kerberos and the location of the keytab where the principal is stored. ",(0,i.kt)("inlineCode",{parentName:"li"},"PulsarClient")," allows the client to use the keytab specified in this section.\nThe following example also reuses this ",(0,i.kt)("inlineCode",{parentName:"li"},"PulsarClient")," section in both the Pulsar internal admin configuration and in CLI command of ",(0,i.kt)("inlineCode",{parentName:"li"},"bin/pulsar-client"),", ",(0,i.kt)("inlineCode",{parentName:"li"},"bin/pulsar-perf")," and ",(0,i.kt)("inlineCode",{parentName:"li"},"bin/pulsar-admin"),". You can also add different sections for different use cases.")),(0,i.kt)("p",null,"You can have 2 separate JAAS configuration files: "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"the file for a broker that has sections of both ",(0,i.kt)("inlineCode",{parentName:"li"},"PulsarBroker")," and ",(0,i.kt)("inlineCode",{parentName:"li"},"PulsarClient"),"; "),(0,i.kt)("li",{parentName:"ul"},"the file for a client that only has a ",(0,i.kt)("inlineCode",{parentName:"li"},"PulsarClient")," section.")),(0,i.kt)("h3",{id:"kerberos-configuration-for-brokers"},"Kerberos configuration for Brokers"),(0,i.kt)("h4",{id:"configure-the-brokerconf-file"},"Configure the ",(0,i.kt)("inlineCode",{parentName:"h4"},"broker.conf")," file"),(0,i.kt)("p",null," In the ",(0,i.kt)("inlineCode",{parentName:"p"},"broker.conf")," file, set Kerberos related configurations."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Set ",(0,i.kt)("inlineCode",{parentName:"p"},"authenticationEnabled")," to ",(0,i.kt)("inlineCode",{parentName:"p"},"true"),";")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Set ",(0,i.kt)("inlineCode",{parentName:"p"},"authenticationProviders")," to choose ",(0,i.kt)("inlineCode",{parentName:"p"},"AuthenticationProviderSasl"),";")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Set ",(0,i.kt)("inlineCode",{parentName:"p"},"saslJaasClientAllowedIds")," regex for principal that is allowed to connect to broker;")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Set ",(0,i.kt)("inlineCode",{parentName:"p"},"saslJaasBrokerSectionName")," that corresponds to the section in JAAS configuration file for broker;"),(0,i.kt)("p",{parentName:"li"},"To make Pulsar internal admin client work properly, you need to set the configuration in the ",(0,i.kt)("inlineCode",{parentName:"p"},"broker.conf")," file as below: ")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Set ",(0,i.kt)("inlineCode",{parentName:"p"},"brokerClientAuthenticationPlugin")," to client plugin ",(0,i.kt)("inlineCode",{parentName:"p"},"AuthenticationSasl"),";")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Set ",(0,i.kt)("inlineCode",{parentName:"p"},"brokerClientAuthenticationParameters")," to value in JSON string ",(0,i.kt)("inlineCode",{parentName:"p"},'{"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}'),", in which ",(0,i.kt)("inlineCode",{parentName:"p"},"PulsarClient")," is the section name in the ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf")," file, and ",(0,i.kt)("inlineCode",{parentName:"p"},'"serverType":"broker"')," indicates that the internal admin client connects to a Pulsar Broker;"),(0,i.kt)("p",{parentName:"li"},"Here is an example:"))),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'\nauthenticationEnabled=true\nauthenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl\nsaslJaasClientAllowedIds=.*client.*\nsaslJaasBrokerSectionName=PulsarBroker\n\n## Authentication settings of the broker itself. Used when the broker connects to other brokers\nbrokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl\nbrokerClientAuthenticationParameters={"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}\n\n')),(0,i.kt)("h4",{id:"set-broker-jvm-parameter"},"Set Broker JVM parameter"),(0,i.kt)("p",null," Set JVM parameters for JAAS configuration file and krb5 configuration file with additional options."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n   -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf\n\n")),(0,i.kt)("p",null,"You can add this at the end of ",(0,i.kt)("inlineCode",{parentName:"p"},"PULSAR_EXTRA_OPTS")," in the file ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/conf/pulsar_env.sh"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar_env.sh"))),(0,i.kt)("p",null,"You must ensure that the operating system user who starts broker can reach the keytabs configured in the ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf")," file and kdc server in the ",(0,i.kt)("inlineCode",{parentName:"p"},"krb5.conf")," file."),(0,i.kt)("h3",{id:"kerberos-configuration-for-clients"},"Kerberos configuration for clients"),(0,i.kt)("h4",{id:"java-client-and-java-admin-client"},"Java Client and Java Admin Client"),(0,i.kt)("p",null,"In client application, include ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar-client-auth-sasl")," in your project dependency."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\n    <dependency>\n      <groupId>org.apache.pulsar</groupId>\n      <artifactId>pulsar-client-auth-sasl</artifactId>\n      <version>${pulsar.version}</version>\n    </dependency>\n\n")),(0,i.kt)("p",null,"Configure the authentication type to use ",(0,i.kt)("inlineCode",{parentName:"p"},"AuthenticationSasl"),", and also provide the authentication parameters to it. "),(0,i.kt)("p",null,"You need 2 parameters: "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"saslJaasClientSectionName"),". This parameter corresponds to the section in JAAS configuration file for client; "),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"serverType"),". This parameter stands for whether this client connects to broker or proxy. And client uses this parameter to know which server side principal should be used. ")),(0,i.kt)("p",null,"When you authenticate between client and broker with the setting in above JAAS configuration file, we need to set ",(0,i.kt)("inlineCode",{parentName:"p"},"saslJaasClientSectionName")," to ",(0,i.kt)("inlineCode",{parentName:"p"},"PulsarClient")," and set ",(0,i.kt)("inlineCode",{parentName:"p"},"serverType")," to ",(0,i.kt)("inlineCode",{parentName:"p"},"broker"),"."),(0,i.kt)("p",null,"The following is an example of creating a Java client:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'\nSystem.setProperty("java.security.auth.login.config", "/etc/pulsar/pulsar_jaas.conf");\nSystem.setProperty("java.security.krb5.conf", "/etc/pulsar/krb5.conf");\n\nMap<String, String> authParams = Maps.newHashMap();\nauthParams.put("saslJaasClientSectionName", "PulsarClient");\nauthParams.put("serverType", "broker");\n\nAuthentication saslAuth = AuthenticationFactory\n        .create(org.apache.pulsar.client.impl.auth.AuthenticationSasl.class.getName(), authParams);\n\nPulsarClient client = PulsarClient.builder()\n        .serviceUrl("pulsar://my-broker.com:6650")\n        .authentication(saslAuth)\n        .build();\n\n')),(0,i.kt)("blockquote",null,(0,i.kt)("p",{parentName:"blockquote"},"The first two lines in the example above are hard coded, alternatively, you can set additional JVM parameters for JAAS and krb5 configuration file when you run the application like below:")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\njava -cp -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf $APP-jar-with-dependencies.jar $CLASSNAME\n\n")),(0,i.kt)("p",null,"You must ensure that the operating system user who starts pulsar client can reach the keytabs configured in the ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf")," file and kdc server in the ",(0,i.kt)("inlineCode",{parentName:"p"},"krb5.conf")," file."),(0,i.kt)("h4",{id:"configure-cli-tools"},"Configure CLI tools"),(0,i.kt)("p",null,"If you use a command-line tool (such as ",(0,i.kt)("inlineCode",{parentName:"p"},"bin/pulsar-client"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"bin/pulsar-perf")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"bin/pulsar-admin"),"), you need to perform the following steps:"),(0,i.kt)("p",null,"Step 1. Enter the command below to configure your ",(0,i.kt)("inlineCode",{parentName:"p"},"client.conf"),"."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},'\nauthPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl\nauthParams={"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}\n\n')),(0,i.kt)("p",null,"Step 2. Enter the command below to set JVM parameters for JAAS configuration file and krb5 configuration file with additional options."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n   -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf\n\n")),(0,i.kt)("p",null,"You can add this at the end of ",(0,i.kt)("inlineCode",{parentName:"p"},"PULSAR_EXTRA_OPTS")," in the file ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/blob/master/conf/pulsar_tools_env.sh"},(0,i.kt)("inlineCode",{parentName:"a"},"pulsar_tools_env.sh")),",\nor add this line ",(0,i.kt)("inlineCode",{parentName:"p"},'OPTS="$OPTS -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf "')," directly to the CLI tool script."),(0,i.kt)("p",null,"The meaning of configurations is the same as the meaning of configurations in Java client section."),(0,i.kt)("h2",{id:"kerberos-configuration-for-working-with-pulsar-proxy"},"Kerberos configuration for working with Pulsar Proxy"),(0,i.kt)("p",null,"With the above configuration, client and broker can do authentication using Kerberos.  "),(0,i.kt)("p",null,"A client that connects to Pulsar Proxy is a little different. Pulsar Proxy (as a SASL Server in Kerberos) authenticates Client (as a SASL client in Kerberos) first; and then Pulsar broker authenticates Pulsar Proxy. "),(0,i.kt)("p",null,"Now in comparison with the above configuration between client and broker, we show you how to configure Pulsar Proxy as follows. "),(0,i.kt)("h3",{id:"create-principal-for-pulsar-proxy-in-kerberos"},"Create principal for Pulsar Proxy in Kerberos"),(0,i.kt)("p",null,"You need to add new principals for Pulsar Proxy comparing with the above configuration. If you already have principals for client and broker, you only need to add the proxy principal here."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"\n### add Principals for Pulsar Proxy\nsudo /usr/sbin/kadmin.local -q 'addprinc -randkey proxy/{hostname}@{REALM}'\nsudo /usr/sbin/kadmin.local -q \"ktadd -k /etc/security/keytabs/{proxy-keytabname}.keytab proxy/{hostname}@{REALM}\"\n### add Principals for broker\nsudo /usr/sbin/kadmin.local -q 'addprinc -randkey broker/{hostname}@{REALM}'\nsudo /usr/sbin/kadmin.local -q \"ktadd -k /etc/security/keytabs/{broker-keytabname}.keytab broker/{hostname}@{REALM}\"\n### add Principals for client\nsudo /usr/sbin/kadmin.local -q 'addprinc -randkey client/{hostname}@{REALM}'\nsudo /usr/sbin/kadmin.local -q \"ktadd -k /etc/security/keytabs/{client-keytabname}.keytab client/{hostname}@{REALM}\"\n\n")),(0,i.kt)("h3",{id:"add-a-section-in-jaas-configuration-file-for-pulsar-proxy"},"Add a section in JAAS configuration file for Pulsar Proxy"),(0,i.kt)("p",null,"In comparison with the above configuration, add a new section for Pulsar Proxy in JAAS configuration file."),(0,i.kt)("p",null,"Here is an example named ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'\n PulsarBroker {\n   com.sun.security.auth.module.Krb5LoginModule required\n   useKeyTab=true\n   storeKey=true\n   useTicketCache=false\n   keyTab="/etc/security/keytabs/pulsarbroker.keytab"\n   principal="broker/localhost@EXAMPLE.COM";\n};\n\n PulsarProxy {\n   com.sun.security.auth.module.Krb5LoginModule required\n   useKeyTab=true\n   storeKey=true\n   useTicketCache=false\n   keyTab="/etc/security/keytabs/pulsarproxy.keytab"\n   principal="proxy/localhost@EXAMPLE.COM";\n};\n\n PulsarClient {\n   com.sun.security.auth.module.Krb5LoginModule required\n   useKeyTab=true\n   storeKey=true\n   useTicketCache=false\n   keyTab="/etc/security/keytabs/pulsarclient.keytab"\n   principal="client/localhost@EXAMPLE.COM";\n};\n\n')),(0,i.kt)("h3",{id:"proxy-client-configuration"},"Proxy client configuration"),(0,i.kt)("p",null,"Pulsar client configuration is similar with client and broker configuration, except that you need to set ",(0,i.kt)("inlineCode",{parentName:"p"},"serverType")," to ",(0,i.kt)("inlineCode",{parentName:"p"},"proxy")," instead of ",(0,i.kt)("inlineCode",{parentName:"p"},"broker"),", for the reason that you need to do the Kerberos authentication between client and proxy."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'\nSystem.setProperty("java.security.auth.login.config", "/etc/pulsar/pulsar_jaas.conf");\nSystem.setProperty("java.security.krb5.conf", "/etc/pulsar/krb5.conf");\n\nMap<String, String> authParams = Maps.newHashMap();\nauthParams.put("saslJaasClientSectionName", "PulsarClient");\nauthParams.put("serverType", "proxy");        // ** here is the different **\n\nAuthentication saslAuth = AuthenticationFactory\n        .create(org.apache.pulsar.client.impl.auth.AuthenticationSasl.class.getName(), authParams);\n\nPulsarClient client = PulsarClient.builder()\n        .serviceUrl("pulsar://my-broker.com:6650")\n        .authentication(saslAuth)\n        .build();\n\n')),(0,i.kt)("blockquote",null,(0,i.kt)("p",{parentName:"blockquote"},"The first two lines in the example above are hard coded, alternatively, you can set additional JVM parameters for JAAS and krb5 configuration file when you run the application like below:")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\njava -cp -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf $APP-jar-with-dependencies.jar $CLASSNAME\n\n")),(0,i.kt)("h3",{id:"kerberos-configuration-for-pulsar-proxy-service"},"Kerberos configuration for Pulsar proxy service"),(0,i.kt)("p",null,"In the ",(0,i.kt)("inlineCode",{parentName:"p"},"proxy.conf")," file, set Kerberos related configuration. Here is an example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},'\n## related to authenticate client.\nauthenticationEnabled=true\nauthenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl\nsaslJaasClientAllowedIds=.*client.*\nsaslJaasBrokerSectionName=PulsarProxy\n\n## related to be authenticated by broker\nbrokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl\nbrokerClientAuthenticationParameters={"saslJaasClientSectionName":"PulsarProxy", "serverType":"broker"}\nforwardAuthorizationCredentials=true\n\n')),(0,i.kt)("p",null,"The first part relates to authenticating between client and Pulsar Proxy. In this phase, client works as SASL client, while Pulsar Proxy works as SASL server. "),(0,i.kt)("p",null,"The second part relates to authenticating between Pulsar Proxy and Pulsar Broker. In this phase, Pulsar Proxy works as SASL client, while Pulsar Broker works as SASL server."),(0,i.kt)("h3",{id:"broker-side-configuration"},"Broker side configuration."),(0,i.kt)("p",null,"The broker side configuration file is the same with the above ",(0,i.kt)("inlineCode",{parentName:"p"},"broker.conf"),", you do not need special configuration for Pulsar Proxy."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\nauthenticationEnabled=true\nauthenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl\nsaslJaasClientAllowedIds=.*client.*\nsaslJaasBrokerSectionName=PulsarBroker\n\n")),(0,i.kt)("h2",{id:"regarding-authorization-and-role-token"},"Regarding authorization and role token"),(0,i.kt)("p",null,"For Kerberos authentication, we usually use the authenticated principal as the role token for Pulsar authorization. For more information of authorization in Pulsar, see ",(0,i.kt)("a",{parentName:"p",href:"security-authorization"},"security authorization"),"."),(0,i.kt)("p",null,"If you enable 'authorizationEnabled', you need to set ",(0,i.kt)("inlineCode",{parentName:"p"},"superUserRoles")," in ",(0,i.kt)("inlineCode",{parentName:"p"},"broker.conf")," that corresponds to the name registered in kdc."),(0,i.kt)("p",null,"For example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"\nsuperUserRoles=client/{clientIp}@EXAMPLE.COM\n\n")),(0,i.kt)("h2",{id:"regarding-authentication-between-zookeeper-and-broker"},"Regarding authentication between ZooKeeper and Broker"),(0,i.kt)("p",null,"Pulsar Broker acts as a Kerberos client when you authenticate with Zookeeper. According to ",(0,i.kt)("a",{parentName:"p",href:"https://cwiki.apache.org/confluence/display/ZOOKEEPER/Client-Server+mutual+authentication"},"ZooKeeper document"),", you need these settings in ",(0,i.kt)("inlineCode",{parentName:"p"},"conf/zookeeper.conf"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\nauthProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider\nrequireClientAuthScheme=sasl\n\n")),(0,i.kt)("p",null,"Enter the following commands to add a section of ",(0,i.kt)("inlineCode",{parentName:"p"},"Client")," configurations in the file ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf"),", which Pulsar Broker uses:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'\n Client {\n   com.sun.security.auth.module.Krb5LoginModule required\n   useKeyTab=true\n   storeKey=true\n   useTicketCache=false\n   keyTab="/etc/security/keytabs/pulsarbroker.keytab"\n   principal="broker/localhost@EXAMPLE.COM";\n};\n\n')),(0,i.kt)("p",null,"In this setting, the principal of Pulsar Broker and keyTab file indicates the role of Broker when you authenticate with ZooKeeper."),(0,i.kt)("h2",{id:"regarding-authentication-between-bookkeeper-and-broker"},"Regarding authentication between BookKeeper and Broker"),(0,i.kt)("p",null,"Pulsar Broker acts as a Kerberos client when you authenticate with Bookie. According to ",(0,i.kt)("a",{parentName:"p",href:"http://bookkeeper.apache.org/docs/latest/security/sasl/"},"BookKeeper document"),", you need to add ",(0,i.kt)("inlineCode",{parentName:"p"},"bookkeeperClientAuthenticationPlugin")," parameter in ",(0,i.kt)("inlineCode",{parentName:"p"},"broker.conf"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"\nbookkeeperClientAuthenticationPlugin=org.apache.bookkeeper.sasl.SASLClientProviderFactory\n\n")),(0,i.kt)("p",null,"In this setting, ",(0,i.kt)("inlineCode",{parentName:"p"},"SASLClientProviderFactory")," creates a BookKeeper SASL client in a Broker, and the Broker uses the created SASL client to authenticate with a Bookie node."),(0,i.kt)("p",null,"Enter the following commands to add a section of ",(0,i.kt)("inlineCode",{parentName:"p"},"BookKeeper")," configurations in the ",(0,i.kt)("inlineCode",{parentName:"p"},"pulsar_jaas.conf")," that Pulsar Broker uses:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'\n BookKeeper {\n   com.sun.security.auth.module.Krb5LoginModule required\n   useKeyTab=true\n   storeKey=true\n   useTicketCache=false\n   keyTab="/etc/security/keytabs/pulsarbroker.keytab"\n   principal="broker/localhost@EXAMPLE.COM";\n};\n\n')),(0,i.kt)("p",null,"In this setting, the principal of Pulsar Broker and keyTab file indicates the role of Broker when you authenticate with Bookie."))}d.isMDXComponent=!0}}]);
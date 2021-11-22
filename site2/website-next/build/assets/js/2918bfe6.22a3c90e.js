"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[89425],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return m}});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var c=r.createContext({}),l=function(e){var t=r.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=l(e.components);return r.createElement(c.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},h=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,c=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),h=l(n),m=a,d=h["".concat(c,".").concat(m)]||h[m]||u[m]||o;return n?r.createElement(d,i(i({ref:t},p),{},{components:n})):r.createElement(d,i({ref:t},p))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=h;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s.mdxType="string"==typeof e?e:a,i[1]=s;for(var l=2;l<o;l++)i[l]=n[l];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}h.displayName="MDXCreateElement"},20358:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return c},metadata:function(){return l},toc:function(){return p},default:function(){return h}});var r=n(87462),a=n(63366),o=(n(67294),n(3905)),i=["components"],s={id:"security-tls-transport",title:"Transport Encryption using TLS",sidebar_label:"Transport Encryption using TLS",original_id:"security-tls-transport"},c=void 0,l={unversionedId:"security-tls-transport",id:"version-2.6.2/security-tls-transport",isDocsHomePage:!1,title:"Transport Encryption using TLS",description:"TLS overview",source:"@site/versioned_docs/version-2.6.2/security-tls-transport.md",sourceDirName:".",slug:"/security-tls-transport",permalink:"/docs/2.6.2/security-tls-transport",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.2/security-tls-transport.md",tags:[],version:"2.6.2",frontMatter:{id:"security-tls-transport",title:"Transport Encryption using TLS",sidebar_label:"Transport Encryption using TLS",original_id:"security-tls-transport"},sidebar:"version-2.6.2/docsSidebar",previous:{title:"Overview",permalink:"/docs/2.6.2/security-overview"},next:{title:"Authentication using TLS",permalink:"/docs/2.6.2/security-tls-authentication"}},p=[{value:"TLS overview",id:"tls-overview",children:[]},{value:"TLS concepts",id:"tls-concepts",children:[]},{value:"Create TLS certificates",id:"create-tls-certificates",children:[{value:"Certificate authority",id:"certificate-authority",children:[]},{value:"Server certificate",id:"server-certificate",children:[]}]},{value:"Broker Configuration",id:"broker-configuration",children:[]},{value:"Proxy Configuration",id:"proxy-configuration",children:[]},{value:"Client configuration",id:"client-configuration",children:[{value:"Hostname verification",id:"hostname-verification",children:[]},{value:"CLI tools",id:"cli-tools",children:[]}]}],u={toc:p};function h(e){var t=e.components,n=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"tls-overview"},"TLS overview"),(0,o.kt)("p",null,"By default, Apache Pulsar clients communicate with the Apache Pulsar service in plain text. This means that all data is sent in the clear. You can use TLS to encrypt this traffic to protect the traffic from the snooping of a man-in-the-middle attacker."),(0,o.kt)("p",null,"You can also configure TLS for both encryption and authentication. Use this guide to configure just TLS transport encryption and refer to ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.6.2/security-tls-authentication"},"here")," for TLS authentication configuration. Alternatively, you can use ",(0,o.kt)("a",{parentName:"p",href:"security-athenz"},"another authentication mechanism")," on top of TLS transport encryption."),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"Note that enabling TLS may impact the performance due to encryption overhead.")),(0,o.kt)("h2",{id:"tls-concepts"},"TLS concepts"),(0,o.kt)("p",null,"TLS is a form of ",(0,o.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Public-key_cryptography"},"public key cryptography"),". Using key pairs consisting of a public key and a private key can perform the encryption. The public key encrpyts the messages and the private key decrypts the messages."),(0,o.kt)("p",null,"To use TLS transport encryption, you need two kinds of key pairs, ",(0,o.kt)("strong",{parentName:"p"},"server key pairs")," and a ",(0,o.kt)("strong",{parentName:"p"},"certificate authority"),"."),(0,o.kt)("p",null,"You can use a third kind of key pair, ",(0,o.kt)("strong",{parentName:"p"},"client key pairs"),", for ",(0,o.kt)("a",{parentName:"p",href:"security-tls-authentication"},"client authentication"),"."),(0,o.kt)("p",null,"You should store the ",(0,o.kt)("strong",{parentName:"p"},"certificate authority")," private key in a very secure location (a fully encrypted, disconnected, air gapped computer). As for the certificate authority public key, the ",(0,o.kt)("strong",{parentName:"p"},"trust cert"),", you can freely shared it."),(0,o.kt)("p",null,"For both client and server key pairs, the administrator first generates a private key and a certificate request, then uses the certificate authority private key to sign the certificate request, finally generates a certificate. This certificate is the public key for the server/client key pair."),(0,o.kt)("p",null,"For TLS transport encryption, the clients can use the ",(0,o.kt)("strong",{parentName:"p"},"trust cert")," to verify that the server has a key pair that the certificate authority signed when the clients are talking to the server. A man-in-the-middle attacker does not have access to the certificate authority, so they couldn't create a server with such a key pair."),(0,o.kt)("p",null,"For TLS authentication, the server uses the ",(0,o.kt)("strong",{parentName:"p"},"trust cert")," to verify that the client has a key pair that the certificate authority signed. The common name of the ",(0,o.kt)("strong",{parentName:"p"},"client cert")," is then used as the client's role token (see ",(0,o.kt)("a",{parentName:"p",href:"security-overview"},"Overview"),")."),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"Bouncy Castle Provider")," provides cipher suites and algorithms in Pulsar. If you need ",(0,o.kt)("a",{parentName:"p",href:"https://www.bouncycastle.org/fips_faq.html"},"FIPS")," version of ",(0,o.kt)("inlineCode",{parentName:"p"},"Bouncy Castle Provider"),", please reference ",(0,o.kt)("a",{parentName:"p",href:"security-bouncy-castle"},"Bouncy Castle page"),"."),(0,o.kt)("h2",{id:"create-tls-certificates"},"Create TLS certificates"),(0,o.kt)("p",null,"Creating TLS certificates for Pulsar involves creating a ",(0,o.kt)("a",{parentName:"p",href:"#certificate-authority"},"certificate authority")," (CA), ",(0,o.kt)("a",{parentName:"p",href:"#server-certificate"},"server certificate"),", and ",(0,o.kt)("a",{parentName:"p",href:"#client-certificate"},"client certificate"),"."),(0,o.kt)("p",null,"Follow the guide below to set up a certificate authority. You can also refer to plenty of resources on the internet for more details. We recommend ",(0,o.kt)("a",{parentName:"p",href:"https://jamielinux.com/docs/openssl-certificate-authority/index.html"},"this guide")," for your detailed reference."),(0,o.kt)("h3",{id:"certificate-authority"},"Certificate authority"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Create the certificate for the CA. You can use CA to sign both the broker and client certificates. This ensures that each party will trust the others. You should store CA in a very secure location (ideally completely disconnected from networks, air gapped, and fully encrypted).")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Entering the following command to create a directory for your CA, and place ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/apache/pulsar/tree/master/site2/website/static/examples/openssl.cnf"},"this openssl configuration file")," in the directory. You may want to modify the default answers for company name and department in the configuration file. Export the location of the CA directory to the environment variable, CA_HOME. The configuration file uses this environment variable to find the rest of the files and directories that the CA needs."))),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nmkdir my-ca\ncd my-ca\nwget https://raw.githubusercontent.com/apache/pulsar/master/site2/website/static/examples/openssl.cnf\nexport CA_HOME=$(pwd)\n\n")),(0,o.kt)("ol",{start:3},(0,o.kt)("li",{parentName:"ol"},"Enter the commands below to create the necessary directories, keys and certs.")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nmkdir certs crl newcerts private\nchmod 700 private/\ntouch index.txt\necho 1000 > serial\nopenssl genrsa -aes256 -out private/ca.key.pem 4096\nchmod 400 private/ca.key.pem\nopenssl req -config openssl.cnf -key private/ca.key.pem \\\n    -new -x509 -days 7300 -sha256 -extensions v3_ca \\\n    -out certs/ca.cert.pem\nchmod 444 certs/ca.cert.pem\n\n")),(0,o.kt)("ol",{start:4},(0,o.kt)("li",{parentName:"ol"},"After you answer the question prompts, CA-related files are stored in the ",(0,o.kt)("inlineCode",{parentName:"li"},"./my-ca")," directory. Within that directory:")),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"certs/ca.cert.pem")," is the public certificate. This public certificates is meant to be distributed to all parties involved."),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"private/ca.key.pem")," is the private key. You only need it when you are signing a new certificate for either broker or clients and you must safely guard this private key.")),(0,o.kt)("h3",{id:"server-certificate"},"Server certificate"),(0,o.kt)("p",null,"Once you have created a CA certificate, you can create certificate requests and sign them with the CA."),(0,o.kt)("p",null,"The following commands ask you a few questions and then create the certificates. When you are asked for the common name, you should match the hostname of the broker. You can also use a wildcard to match a group of broker hostnames, for example, ",(0,o.kt)("inlineCode",{parentName:"p"},"*.broker.usw.example.com"),". This ensures that multiple machines can reuse the same certificate."),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"Sometimes matching the hostname is not possible or makes no sense,\nsuch as when you create the brokers with random hostnames, or you\nplan to connect to the hosts via their IP. In these cases, you\nshould configure the client to disable TLS hostname verification. For more\ndetails, you can see ",(0,o.kt)("a",{parentName:"p",href:"#hostname-verification"},"the host verification section in client configuration"),"."))),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Enter the command below to generate the key.")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nopenssl genrsa -out broker.key.pem 2048\n\n")),(0,o.kt)("p",null,"The broker expects the key to be in ",(0,o.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/PKCS_8"},"PKCS 8")," format, so enter the following command to convert it."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nopenssl pkcs8 -topk8 -inform PEM -outform PEM \\\n      -in broker.key.pem -out broker.key-pk8.pem -nocrypt\n\n")),(0,o.kt)("ol",{start:2},(0,o.kt)("li",{parentName:"ol"},"Enter the following command to generate the certificate request.")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nopenssl req -config openssl.cnf \\\n    -key broker.key.pem -new -sha256 -out broker.csr.pem\n\n")),(0,o.kt)("ol",{start:3},(0,o.kt)("li",{parentName:"ol"},"Sign it with the certificate authority by entering the command below.")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nopenssl ca -config openssl.cnf -extensions server_cert \\\n    -days 1000 -notext -md sha256 \\\n    -in broker.csr.pem -out broker.cert.pem\n\n")),(0,o.kt)("p",null,"At this point, you have a cert, ",(0,o.kt)("inlineCode",{parentName:"p"},"broker.cert.pem"),", and a key, ",(0,o.kt)("inlineCode",{parentName:"p"},"broker.key-pk8.pem"),", which you can use along with ",(0,o.kt)("inlineCode",{parentName:"p"},"ca.cert.pem")," to configure TLS transport encryption for your broker and proxy nodes."),(0,o.kt)("h2",{id:"broker-configuration"},"Broker Configuration"),(0,o.kt)("p",null,"To configure a Pulsar ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-terminology#broker"},"broker")," to use TLS transport encryption, you need to make some changes to ",(0,o.kt)("inlineCode",{parentName:"p"},"broker.conf"),", which locates in the ",(0,o.kt)("inlineCode",{parentName:"p"},"conf")," directory of your ",(0,o.kt)("a",{parentName:"p",href:"getting-started-standalone"},"Pulsar installation"),"."),(0,o.kt)("p",null,"Add these values to the configuration file (substituting the appropriate certificate paths where necessary):"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-properties"},"\ntlsEnabled=true\ntlsCertificateFilePath=/path/to/broker.cert.pem\ntlsKeyFilePath=/path/to/broker.key-pk8.pem\ntlsTrustCertsFilePath=/path/to/ca.cert.pem\n\n")),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"You can find a full list of parameters available in the ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/broker.conf")," file,\nas well as the default values for those parameters, in ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-configuration#broker"},"Broker Configuration")," "),(0,o.kt)("h3",{parentName:"blockquote",id:"tls-protocol-version-and-cipher"},"TLS Protocol Version and Cipher")),(0,o.kt)("p",null,"You can configure the broker (and proxy) to require specific TLS protocol versions and ciphers for TLS negiotation. You can use the TLS protocol versions and ciphers to stop clients from requesting downgraded TLS protocol versions or ciphers that may have weaknesses."),(0,o.kt)("p",null,"Both the TLS protocol versions and cipher properties can take multiple values, separated by commas. The possible values for protocol version and ciphers depend on the TLS provider that you are using. Pulsar uses OpenSSL if the OpenSSL is available, but if the OpenSSL is not available, Pulsar defaults back to the JDK implementation."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-properties"},"\ntlsProtocols=TLSv1.2,TLSv1.1\ntlsCiphers=TLS_DH_RSA_WITH_AES_256_GCM_SHA384,TLS_DH_RSA_WITH_AES_256_CBC_SHA\n\n")),(0,o.kt)("p",null,"OpenSSL currently supports ",(0,o.kt)("inlineCode",{parentName:"p"},"SSL2"),", ",(0,o.kt)("inlineCode",{parentName:"p"},"SSL3"),", ",(0,o.kt)("inlineCode",{parentName:"p"},"TLSv1"),", ",(0,o.kt)("inlineCode",{parentName:"p"},"TLSv1.1")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"TLSv1.2")," for the protocol version. You can acquire a list of supported cipher from the openssl ciphers command, i.e. ",(0,o.kt)("inlineCode",{parentName:"p"},"openssl ciphers -tls_v2"),"."),(0,o.kt)("p",null,"For JDK 8, you can obtain a list of supported values from the documentation:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext"},"TLS protocol")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites"},"Ciphers"))),(0,o.kt)("h2",{id:"proxy-configuration"},"Proxy Configuration"),(0,o.kt)("p",null,"Proxies need to configure TLS in two directions, for clients connecting to the proxy, and for the proxy connecting to brokers."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-properties"},"\n# For clients connecting to the proxy\ntlsEnabledInProxy=true\ntlsCertificateFilePath=/path/to/broker.cert.pem\ntlsKeyFilePath=/path/to/broker.key-pk8.pem\ntlsTrustCertsFilePath=/path/to/ca.cert.pem\n\n# For the proxy to connect to brokers\ntlsEnabledWithBroker=true\nbrokerClientTrustCertsFilePath=/path/to/ca.cert.pem\n\n")),(0,o.kt)("h2",{id:"client-configuration"},"Client configuration"),(0,o.kt)("p",null,"When you enable the TLS transport encryption, you need to configure the client to use ",(0,o.kt)("inlineCode",{parentName:"p"},"https://")," and port 8443 for the web service URL, and ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar+ssl://")," and port 6651 for the broker service URL."),(0,o.kt)("p",null,"As the server certificate that you generated above does not belong to any of the default trust chains, you also need to either specify the path the ",(0,o.kt)("strong",{parentName:"p"},"trust cert")," (recommended), or tell the client to allow untrusted server certs."),(0,o.kt)("h3",{id:"hostname-verification"},"Hostname verification"),(0,o.kt)("p",null,'Hostname verification is a TLS security feature whereby a client can refuse to connect to a server if the "CommonName" does not match the hostname to which the hostname is connecting. By default, Pulsar clients disable hostname verification, as it requires that each broker has a DNS record and a unique cert.'),(0,o.kt)("p",null,'Moreover, as the administrator has full control of the certificate authority, a bad actor is unlikely to be able to pull off a man-in-the-middle attack. "allowInsecureConnection" allows the client to connect to servers whose cert has not been signed by an approved CA. The client disables "allowInsecureConnection" by default, and you should always disable "allowInsecureConnection" in production environments. As long as you disable "allowInsecureConnection", a man-in-the-middle attack requires that the attacker has access to the CA.'),(0,o.kt)("p",null,'One scenario where you may want to enable hostname verification is where you have multiple proxy nodes behind a VIP, and the VIP has a DNS record, for example, pulsar.mycompany.com. In this case, you can generate a TLS cert with pulsar.mycompany.com as the "CommonName," and then enable hostname verification on the client.'),(0,o.kt)("p",null,"The examples below show hostname verification being disabled for the Java client, though you can omit this as the client disables the hostname verification by default. C++/python/Node.js clients do now allow configuring this at the moment."),(0,o.kt)("h3",{id:"cli-tools"},"CLI tools"),(0,o.kt)("p",null,(0,o.kt)("a",{parentName:"p",href:"reference-cli-tools"},"Command-line tools")," like ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-cli-tools#pulsar-admin"},(0,o.kt)("inlineCode",{parentName:"a"},"pulsar-admin")),", ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-cli-tools#pulsar-perf"},(0,o.kt)("inlineCode",{parentName:"a"},"pulsar-perf")),", and ",(0,o.kt)("a",{parentName:"p",href:"/docs/2.6.2/reference-cli-tools#pulsar-client"},(0,o.kt)("inlineCode",{parentName:"a"},"pulsar-client"))," use the ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/client.conf")," config file in a Pulsar installation."),(0,o.kt)("p",null,"You need to add the following parameters to that file to use TLS transport with the CLI tools of Pulsar:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-properties"},"\nwebServiceUrl=https://broker.example.com:8443/\nbrokerServiceUrl=pulsar+ssl://broker.example.com:6651/\nuseTls=true\ntlsAllowInsecureConnection=false\ntlsTrustCertsFilePath=/path/to/ca.cert.pem\ntlsEnableHostnameVerification=false\n\n")),(0,o.kt)("h4",{id:"java-client"},"Java client"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},'\nimport org.apache.pulsar.client.api.PulsarClient;\n\nPulsarClient client = PulsarClient.builder()\n    .serviceUrl("pulsar+ssl://broker.example.com:6651/")\n    .enableTls(true)\n    .tlsTrustCertsFilePath("/path/to/ca.cert.pem")\n    .enableTlsHostnameVerification(false) // false by default, in any case\n    .allowTlsInsecureConnection(false) // false by default, in any case\n    .build();\n\n')),(0,o.kt)("h4",{id:"python-client"},"Python client"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'\nfrom pulsar import Client\n\nclient = Client("pulsar+ssl://broker.example.com:6651/",\n                tls_hostname_verification=True,\n                tls_trust_certs_file_path="/path/to/ca.cert.pem",\n                tls_allow_insecure_connection=False) // defaults to false from v2.2.0 onwards\n\n')),(0,o.kt)("h4",{id:"c-client"},"C++ client"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-c++"},"\n#include <pulsar/Client.h>\n\nClientConfiguration config = ClientConfiguration();\nconfig.setUseTls(true);  // shouldn't be needed soon\nconfig.setTlsTrustCertsFilePath(caPath);\nconfig.setTlsAllowInsecureConnection(false);\nconfig.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));\nconfig.setValidateHostName(true);\n\n")),(0,o.kt)("h4",{id:"nodejs-client"},"Node.js client"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-JavaScript"},"\nconst Pulsar = require('pulsar-client');\n\n(async () => {\n  const client = new Pulsar.Client({\n    serviceUrl: 'pulsar+ssl://broker.example.com:6651/',\n    tlsTrustCertsFilePath: '/path/to/ca.cert.pem',\n  });\n})();\n\n")),(0,o.kt)("h4",{id:"c-client-1"},"C# client"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-c#"},"\nvar certificate = new X509Certificate2(\"ca.cert.pem\");\nvar client = PulsarClient.Builder()\n                         .TrustedCertificateAuthority(certificate) //If the CA is not trusted on the host, you can add it explicitly.\n                         .VerifyCertificateAuthority(true) //Default is 'true'\n                         .VerifyCertificateName(false)     //Default is 'false'\n                         .Build();\n\n")))}h.isMDXComponent=!0}}]);
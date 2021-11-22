"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[10016],{3905:function(e,t,a){a.d(t,{Zo:function(){return d},kt:function(){return c}});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),p=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},d=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,d=l(e,["components","mdxType","originalType","parentName"]),u=p(a),c=r,f=u["".concat(s,".").concat(c)]||u[c]||m[c]||o;return a?n.createElement(f,i(i({ref:t},d),{},{components:a})):n.createElement(f,i({ref:t},d))}));function c(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,i=new Array(o);i[0]=u;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:r,i[1]=l;for(var p=2;p<o;p++)i[p]=a[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}u.displayName="MDXCreateElement"},57375:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return l},contentTitle:function(){return s},metadata:function(){return p},toc:function(){return d},default:function(){return u}});var n=a(87462),r=a(63366),o=(a(67294),a(3905)),i=["components"],l={id:"tiered-storage-aws",title:"Use AWS S3 offloader with Pulsar",sidebar_label:"AWS S3 offloader",original_id:"tiered-storage-aws"},s=void 0,p={unversionedId:"tiered-storage-aws",id:"version-2.7.3/tiered-storage-aws",isDocsHomePage:!1,title:"Use AWS S3 offloader with Pulsar",description:"This chapter guides you through every step of installing and configuring the AWS S3 offloader and using it with Pulsar.",source:"@site/versioned_docs/version-2.7.3/tiered-storage-aws.md",sourceDirName:".",slug:"/tiered-storage-aws",permalink:"/docs/2.7.3/tiered-storage-aws",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.7.3/tiered-storage-aws.md",tags:[],version:"2.7.3",frontMatter:{id:"tiered-storage-aws",title:"Use AWS S3 offloader with Pulsar",sidebar_label:"AWS S3 offloader",original_id:"tiered-storage-aws"},sidebar:"version-2.7.3/docsSidebar",previous:{title:"Overview",permalink:"/docs/2.7.3/tiered-storage-overview"},next:{title:"GCS offloader",permalink:"/docs/2.7.3/tiered-storage-gcs"}},d=[{value:"Installation",id:"installation",children:[{value:"Prerequisite",id:"prerequisite",children:[]},{value:"Step",id:"step",children:[]}]},{value:"Configuration",id:"configuration",children:[{value:"Configure AWS S3 offloader driver",id:"configure-aws-s3-offloader-driver",children:[]},{value:"Configure AWS S3 offloader to run automatically",id:"configure-aws-s3-offloader-to-run-automatically",children:[]},{value:"Configure AWS S3 offloader to run manually",id:"configure-aws-s3-offloader-to-run-manually",children:[]}]},{value:"Tutorial",id:"tutorial",children:[]}],m={toc:d};function u(e){var t=e.components,a=(0,r.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},m,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"This chapter guides you through every step of installing and configuring the AWS S3 offloader and using it with Pulsar."),(0,o.kt)("h2",{id:"installation"},"Installation"),(0,o.kt)("p",null,"Follow the steps below to install the AWS S3 offloader."),(0,o.kt)("h3",{id:"prerequisite"},"Prerequisite"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Pulsar: 2.4.2 or later versions")),(0,o.kt)("h3",{id:"step"},"Step"),(0,o.kt)("p",null,"This example uses Pulsar 2.5.1."),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Download the Pulsar tarball using one of the following ways:"),(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Download from the ",(0,o.kt)("a",{parentName:"p",href:"https://archive.apache.org/dist/pulsar/pulsar-2.5.1/apache-pulsar-2.5.1-bin.tar.gz"},"Apache mirror"))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Download from the Pulsar ",(0,o.kt)("a",{parentName:"p",href:"https://pulsar.apache.org/download"},"downloads page"))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Use ",(0,o.kt)("a",{parentName:"p",href:"https://www.gnu.org/software/wget"},"wget"),":"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"\nwget https://archive.apache.org/dist/pulsar/pulsar-2.5.1/apache-pulsar-2.5.1-bin.tar.gz\n\n"))))),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Download and untar the Pulsar offloaders package. "),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nwget https://downloads.apache.org/pulsar/pulsar-2.5.1/apache-pulsar-offloaders-2.5.1-bin.tar.gz\ntar xvfz apache-pulsar-offloaders-2.5.1-bin.tar.gz\n\n"))),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Copy the Pulsar offloaders as ",(0,o.kt)("inlineCode",{parentName:"p"},"offloaders")," in the Pulsar directory."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"\nmv apache-pulsar-offloaders-2.5.1/offloaders apache-pulsar-2.5.1/offloaders\n\nls offloaders\n\n")),(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"Output")),(0,o.kt)("p",{parentName:"li"},"As shown from the output, Pulsar uses ",(0,o.kt)("a",{parentName:"p",href:"https://jclouds.apache.org"},"Apache jclouds")," to support ",(0,o.kt)("a",{parentName:"p",href:"https://aws.amazon.com/s3/"},"AWS S3")," and ",(0,o.kt)("a",{parentName:"p",href:"https://cloud.google.com/storage/"},"GCS")," for long term storage. "),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"\ntiered-storage-file-system-2.5.1.nar\ntiered-storage-jcloud-2.5.1.nar\n\n")),(0,o.kt)("div",{parentName:"li",className:"admonition admonition-note alert alert--secondary"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("ul",{parentName:"div"},(0,o.kt)("li",{parentName:"ul"},"If you are running Pulsar in a bare metal cluster, make sure that ",(0,o.kt)("inlineCode",{parentName:"li"},"offloaders")," tarball is unzipped in every broker's Pulsar directory."),(0,o.kt)("li",{parentName:"ul"},"If you are running Pulsar in Docker or deploying Pulsar using a Docker image (such as K8s and DCOS), you can use the ",(0,o.kt)("inlineCode",{parentName:"li"},"apachepulsar/pulsar-all")," image instead of the ",(0,o.kt)("inlineCode",{parentName:"li"},"apachepulsar/pulsar")," image. ",(0,o.kt)("inlineCode",{parentName:"li"},"apachepulsar/pulsar-all")," image has already bundled tiered storage offloaders.")))))),(0,o.kt)("h2",{id:"configuration"},"Configuration"),(0,o.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"Before offloading data from BookKeeper to AWS S3, you need to configure some properties of the AWS S3 offload driver."))),(0,o.kt)("p",null,"Besides, you can also configure the AWS S3 offloader to run it automatically or trigger it manually."),(0,o.kt)("h3",{id:"configure-aws-s3-offloader-driver"},"Configure AWS S3 offloader driver"),(0,o.kt)("p",null,"You can configure the AWS S3 offloader driver in the configuration file ",(0,o.kt)("inlineCode",{parentName:"p"},"broker.conf")," or ",(0,o.kt)("inlineCode",{parentName:"p"},"standalone.conf"),"."),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"Required")," configurations are as below."),(0,o.kt)("table",{parentName:"li"},(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Required configuration"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"),(0,o.kt)("th",{parentName:"tr",align:null},"Example value"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"managedLedgerOffloadDriver")),(0,o.kt)("td",{parentName:"tr",align:null},"Offloader driver name, which is case-insensitive. ",(0,o.kt)("br",null),(0,o.kt)("br",null),(0,o.kt)("strong",{parentName:"td"},"Note"),": there is a third driver type, S3, which is identical to AWS S3, though S3 requires that you specify an endpoint URL using ",(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadServiceEndpoint"),". This is useful if using an S3 compatible data store other than AWS S3."),(0,o.kt)("td",{parentName:"tr",align:null},"aws-s3")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"offloadersDirectory")),(0,o.kt)("td",{parentName:"tr",align:null},"Offloader directory"),(0,o.kt)("td",{parentName:"tr",align:null},"offloaders")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadBucket")),(0,o.kt)("td",{parentName:"tr",align:null},"Bucket"),(0,o.kt)("td",{parentName:"tr",align:null},"pulsar-topic-offload"))))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"Optional")," configurations are as below."),(0,o.kt)("table",{parentName:"li"},(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Optional"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"),(0,o.kt)("th",{parentName:"tr",align:null},"Example value"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadRegion")),(0,o.kt)("td",{parentName:"tr",align:null},"Bucket region ",(0,o.kt)("br",null),(0,o.kt)("br",null),(0,o.kt)("strong",{parentName:"td"},"Note"),": before specifying a value for this parameter, you need to set the following configurations. Otherwise, you might get an error.",(0,o.kt)("br",null),(0,o.kt)("br",null),"- Set ",(0,o.kt)("a",{parentName:"td",href:"https://docs.aws.amazon.com/general/latest/gr/s3.html"},(0,o.kt)("inlineCode",{parentName:"a"},"s3ManagedLedgerOffloadServiceEndpoint")),".",(0,o.kt)("br",null),(0,o.kt)("br",null),"Example",(0,o.kt)("br",null),(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadServiceEndpoint=https://s3.YOUR_REGION.amazonaws.com"),(0,o.kt)("br",null),(0,o.kt)("br",null),"- Grant ",(0,o.kt)("inlineCode",{parentName:"td"},"GetBucketLocation")," permission to a user.",(0,o.kt)("br",null),(0,o.kt)("br",null),"For how to grant ",(0,o.kt)("inlineCode",{parentName:"td"},"GetBucketLocation")," permission to a user, see ",(0,o.kt)("a",{parentName:"td",href:"https://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html#using-with-s3-actions-related-to-buckets"},"here"),"."),(0,o.kt)("td",{parentName:"tr",align:null},"eu-west-3")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadReadBufferSizeInBytes")),(0,o.kt)("td",{parentName:"tr",align:null},"Size of block read"),(0,o.kt)("td",{parentName:"tr",align:null},"1 MB")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadMaxBlockSizeInBytes")),(0,o.kt)("td",{parentName:"tr",align:null},"Size of block write"),(0,o.kt)("td",{parentName:"tr",align:null},"64 MB")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"managedLedgerMinLedgerRolloverTimeMinutes")),(0,o.kt)("td",{parentName:"tr",align:null},"Minimum time between ledger rollover for a topic",(0,o.kt)("br",null),(0,o.kt)("br",null),(0,o.kt)("strong",{parentName:"td"},"Note"),": it is not recommended that you set this configuration in the production environment."),(0,o.kt)("td",{parentName:"tr",align:null},"2")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"managedLedgerMaxEntriesPerLedger")),(0,o.kt)("td",{parentName:"tr",align:null},"Maximum number of entries to append to a ledger before triggering a rollover.",(0,o.kt)("br",null),(0,o.kt)("br",null),(0,o.kt)("strong",{parentName:"td"},"Note"),": it is not recommended that you set this configuration in the production environment."),(0,o.kt)("td",{parentName:"tr",align:null},"5000")))))),(0,o.kt)("h4",{id:"bucket-required"},"Bucket (required)"),(0,o.kt)("p",null,"A bucket is a basic container that holds your data. Everything you store in AWS S3 must be contained in a bucket. You can use a bucket to organize your data and control access to your data, but unlike directory and folder, you cannot nest a bucket."),(0,o.kt)("h5",{id:"example"},"Example"),(0,o.kt)("p",null,"This example names the bucket as ",(0,o.kt)("em",{parentName:"p"},"pulsar-topic-offload"),"."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-conf"},"\ns3ManagedLedgerOffloadBucket=pulsar-topic-offload\n\n")),(0,o.kt)("h4",{id:"bucket-region"},"Bucket region"),(0,o.kt)("p",null,"A bucket region is a region where a bucket is located. If a bucket region is not specified, the ",(0,o.kt)("strong",{parentName:"p"},"default")," region (",(0,o.kt)("inlineCode",{parentName:"p"},"US East (N. Virginia)"),") is used."),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about AWS regions and endpoints, see ",(0,o.kt)("a",{parentName:"p",href:"https://docs.aws.amazon.com/general/latest/gr/rande.html"},"here"),"."))),(0,o.kt)("h5",{id:"example-1"},"Example"),(0,o.kt)("p",null,"This example sets the bucket region as ",(0,o.kt)("em",{parentName:"p"},"europe-west-3"),"."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"\ns3ManagedLedgerOffloadRegion=eu-west-3\n\n")),(0,o.kt)("h4",{id:"authentication-required"},"Authentication (required)"),(0,o.kt)("p",null,"To be able to access AWS S3, you need to authenticate with AWS S3."),(0,o.kt)("p",null,"Pulsar does not provide any direct methods of configuring authentication for AWS S3,\nbut relies on the mechanisms supported by the ",(0,o.kt)("a",{parentName:"p",href:"https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html"},"DefaultAWSCredentialsProviderChain"),"."),(0,o.kt)("p",null,"Once you have created a set of credentials in the AWS IAM console, you can configure credentials using one of the following methods."),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Use EC2 instance metadata credentials."),(0,o.kt)("p",{parentName:"li"},"If you are on AWS instance with an instance profile that provides credentials, Pulsar uses these credentials if no other mechanism is provided.")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Set the environment variables ",(0,o.kt)("inlineCode",{parentName:"p"},"AWS_ACCESS_KEY_ID")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"AWS_SECRET_ACCESS_KEY")," in ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/pulsar_env.sh"),"."),(0,o.kt)("p",{parentName:"li"},'"export" is important so that the variables are made available in the environment of spawned processes.'),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nexport AWS_ACCESS_KEY_ID=ABC123456789\nexport AWS_SECRET_ACCESS_KEY=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c\n\n"))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Add the Java system properties ",(0,o.kt)("inlineCode",{parentName:"p"},"aws.accessKeyId")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"aws.secretKey")," to ",(0,o.kt)("inlineCode",{parentName:"p"},"PULSAR_EXTRA_OPTS")," in ",(0,o.kt)("inlineCode",{parentName:"p"},"conf/pulsar_env.sh"),"."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},'\nPULSAR_EXTRA_OPTS="${PULSAR_EXTRA_OPTS} ${PULSAR_MEM} ${PULSAR_GC} -Daws.accessKeyId=ABC123456789 -Daws.secretKey=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"\n\n'))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Set the access credentials in ",(0,o.kt)("inlineCode",{parentName:"p"},"~/.aws/credentials"),"."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-conf"},"\n[default]\naws_access_key_id=ABC123456789\naws_secret_access_key=ded7db27a4558e2ea8bbf0bf37ae0e8521618f366c\n\n"))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Assume an IAM role."),(0,o.kt)("p",{parentName:"li"},"This example uses the ",(0,o.kt)("inlineCode",{parentName:"p"},"DefaultAWSCredentialsProviderChain")," for assuming this role."),(0,o.kt)("p",{parentName:"li"},"The broker must be rebooted for credentials specified in ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar_env")," to take effect."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-conf"},"\ns3ManagedLedgerOffloadRole=<aws role arn>\ns3ManagedLedgerOffloadRoleSessionName=pulsar-s3-offload\n\n")))),(0,o.kt)("h4",{id:"size-of-block-readwrite"},"Size of block read/write"),(0,o.kt)("p",null,"You can configure the size of a request sent to or read from AWS S3 in the configuration file ",(0,o.kt)("inlineCode",{parentName:"p"},"broker.conf")," or ",(0,o.kt)("inlineCode",{parentName:"p"},"standalone.conf"),". "),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Configuration"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"),(0,o.kt)("th",{parentName:"tr",align:null},"Default value"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadReadBufferSizeInBytes")),(0,o.kt)("td",{parentName:"tr",align:null},"Block size for each individual read when reading back data from AWS S3."),(0,o.kt)("td",{parentName:"tr",align:null},"1 MB")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},"s3ManagedLedgerOffloadMaxBlockSizeInBytes")),(0,o.kt)("td",{parentName:"tr",align:null},'Maximum size of a "part" sent during a multipart upload to AWS S3. It ',(0,o.kt)("strong",{parentName:"td"},"cannot")," be smaller than 5 MB."),(0,o.kt)("td",{parentName:"tr",align:null},"64 MB")))),(0,o.kt)("h3",{id:"configure-aws-s3-offloader-to-run-automatically"},"Configure AWS S3 offloader to run automatically"),(0,o.kt)("p",null,"Namespace policy can be configured to offload data automatically once a threshold is reached. The threshold is based on the size of data that a topic has stored on a Pulsar cluster. Once the topic reaches the threshold, an offloading operation is triggered automatically. "),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Threshold value"),(0,o.kt)("th",{parentName:"tr",align:null},"Action"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"> 0"),(0,o.kt)("td",{parentName:"tr",align:null},"It triggers the offloading operation if the topic storage reaches its threshold.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"= 0"),(0,o.kt)("td",{parentName:"tr",align:null},"It causes a broker to offload data as soon as possible.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"< 0"),(0,o.kt)("td",{parentName:"tr",align:null},"It disables automatic offloading operation.")))),(0,o.kt)("p",null,"Automatic offloading runs when a new segment is added to a topic log. If you set the threshold on a namespace, but few messages are being produced to the topic, offloader does not work until the current segment is full."),(0,o.kt)("p",null,"You can configure the threshold size using CLI tools, such as pulsar-admin."),(0,o.kt)("p",null,"The offload configurations in ",(0,o.kt)("inlineCode",{parentName:"p"},"broker.conf")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"standalone.conf")," are used for the namespaces that do not have namespace level offload policies. Each namespace can have its own offload policy. If you want to set offload policy for each namespace, use the command ",(0,o.kt)("a",{parentName:"p",href:"https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-set-offload-policies-em-"},(0,o.kt)("inlineCode",{parentName:"a"},"pulsar-admin namespaces set-offload-policies options"))," command."),(0,o.kt)("h4",{id:"example-2"},"Example"),(0,o.kt)("p",null,"This example sets the AWS S3 offloader threshold size to 10 MB using pulsar-admin."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin namespaces set-offload-threshold --size 10M my-tenant/my-namespace\n\n")),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about the ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin namespaces set-offload-threshold options")," command, including flags, descriptions, and default values, see ",(0,o.kt)("a",{parentName:"p",href:"https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-set-offload-threshold-em-"},"here"),". "))),(0,o.kt)("h3",{id:"configure-aws-s3-offloader-to-run-manually"},"Configure AWS S3 offloader to run manually"),(0,o.kt)("p",null,"For individual topics, you can trigger AWS S3 offloader manually using one of the following methods:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Use REST endpoint.")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"Use CLI tools (such as pulsar-admin). "),(0,o.kt)("p",{parentName:"li"},"To trigger it via CLI tools, you need to specify the maximum amount of data (threshold) that should be retained on a Pulsar cluster for a topic. If the size of the topic data on the Pulsar cluster exceeds this threshold, segments from the topic are moved to AWS S3 until the threshold is no longer exceeded. Older segments are moved first."))),(0,o.kt)("h4",{id:"example-3"},"Example"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"This example triggers the AWS S3 offloader to run manually using pulsar-admin."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin topics offload --size-threshold 10M my-tenant/my-namespace/topic1\n\n")),(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"Output")),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nOffload triggered for persistent://my-tenant/my-namespace/topic1 for messages before 2:0:-1\n\n")),(0,o.kt)("div",{parentName:"li",className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about the ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin topics offload options")," command, including flags, descriptions, and default values, see ",(0,o.kt)("a",{parentName:"p",href:"https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-offload-em-"},"here"),". ")))),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"This example checks the AWS S3 offloader status using pulsar-admin."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1\n\n")),(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"Output")),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nOffload is currently running\n\n")),(0,o.kt)("p",{parentName:"li"},"To wait for the AWS S3 offloader to complete the job, add the ",(0,o.kt)("inlineCode",{parentName:"p"},"-w")," flag."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin topics offload-status -w persistent://my-tenant/my-namespace/topic1\n\n")),(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"Output")),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"\nOffload was a success\n\n")),(0,o.kt)("p",{parentName:"li"},"If there is an error in offloading, the error is propagated to the ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin topics offload-status")," command."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"\nbin/pulsar-admin topics offload-status persistent://my-tenant/my-namespace/topic1\n\n")),(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"Output")),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"\nError in offload\nnull\n\nReason: Error offloading: org.apache.bookkeeper.mledger.ManagedLedgerException: java.util.concurrent.CompletionException: com.amazonaws.services.s3.model.AmazonS3Exception: Anonymous users cannot initiate multipart uploads.  Please authenticate. (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 798758DE3F1776DF; S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=), S3 Extended Request ID: dhBFz/lZm1oiG/oBEepeNlhrtsDlzoOhocuYMpKihQGXe6EG8puRGOkK6UwqzVrMXTWBxxHcS+g=\n\n")),(0,o.kt)("div",{parentName:"li",className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"For more information about the ",(0,o.kt)("inlineCode",{parentName:"p"},"pulsar-admin topics offload-status options")," command, including flags, descriptions, and default values, see ",(0,o.kt)("a",{parentName:"p",href:"https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-offload-status-em-"},"here"),". "))))),(0,o.kt)("h2",{id:"tutorial"},"Tutorial"),(0,o.kt)("p",null,"For the complete and step-by-step instructions on how to use the AWS S3 offloader with Pulsar, see ",(0,o.kt)("a",{parentName:"p",href:"https://hub.streamnative.io/offloaders/aws-s3/2.5.1#usage"},"here"),"."))}u.isMDXComponent=!0}}]);
"use strict";(self.webpackChunkwebsite_next=self.webpackChunkwebsite_next||[]).push([[90561],{3905:function(e,t,a){a.d(t,{Zo:function(){return m},kt:function(){return u}});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},l=Object.keys(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),d=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},m=function(e){var t=d(e.components);return n.createElement(s.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},c=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,l=e.originalType,s=e.parentName,m=o(e,["components","mdxType","originalType","parentName"]),c=d(a),u=r,h=c["".concat(s,".").concat(u)]||c[u]||p[u]||l;return a?n.createElement(h,i(i({ref:t},m),{},{components:a})):n.createElement(h,i({ref:t},m))}));function u(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=a.length,i=new Array(l);i[0]=c;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:r,i[1]=o;for(var d=2;d<l;d++)i[d]=a[d];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}c.displayName="MDXCreateElement"},55967:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return o},contentTitle:function(){return s},metadata:function(){return d},toc:function(){return m},default:function(){return c}});var n=a(87462),r=a(63366),l=(a(67294),a(3905)),i=["components"],o={id:"schema-evolution-compatibility",title:"Schema evolution and compatibility",sidebar_label:"Schema evolution and compatibility",original_id:"schema-evolution-compatibility"},s=void 0,d={unversionedId:"schema-evolution-compatibility",id:"version-2.6.2/schema-evolution-compatibility",isDocsHomePage:!1,title:"Schema evolution and compatibility",description:"Normally, schemas do not stay the same over a long period of time. Instead, they undergo evolutions to satisfy new needs.",source:"@site/versioned_docs/version-2.6.2/schema-evolution-compatibility.md",sourceDirName:".",slug:"/schema-evolution-compatibility",permalink:"/docs/2.6.2/schema-evolution-compatibility",editUrl:"https://github.com/apache/pulsar/edit/master/site2/website-next/versioned_docs/version-2.6.2/schema-evolution-compatibility.md",tags:[],version:"2.6.2",frontMatter:{id:"schema-evolution-compatibility",title:"Schema evolution and compatibility",sidebar_label:"Schema evolution and compatibility",original_id:"schema-evolution-compatibility"},sidebar:"version-2.6.2/docsSidebar",previous:{title:"Understand schema",permalink:"/docs/2.6.2/schema-understand"},next:{title:"Manage schema",permalink:"/docs/2.6.2/schema-manage"}},m=[{value:"Schema evolution",id:"schema-evolution",children:[{value:"What is schema evolution?",id:"what-is-schema-evolution",children:[]},{value:"How Pulsar schema should evolve?",id:"how-pulsar-schema-should-evolve",children:[]},{value:"How does Pulsar support schema evolution?",id:"how-does-pulsar-support-schema-evolution",children:[]}]},{value:"Schema compatibility check strategy",id:"schema-compatibility-check-strategy",children:[{value:"ALWAYS_COMPATIBLE and ALWAYS_INCOMPATIBLE",id:"always_compatible-and-always_incompatible",children:[]},{value:"BACKWARD and BACKWARD_TRANSITIVE",id:"backward-and-backward_transitive",children:[]},{value:"FORWARD and FORWARD_TRANSITIVE",id:"forward-and-forward_transitive",children:[]},{value:"FULL and FULL_TRANSITIVE",id:"full-and-full_transitive",children:[]}]},{value:"Schema verification",id:"schema-verification",children:[{value:"Producer",id:"producer",children:[]},{value:"Consumer",id:"consumer",children:[]}]},{value:"Order of upgrading clients",id:"order-of-upgrading-clients",children:[]}],p={toc:m};function c(e){var t=e.components,a=(0,r.Z)(e,i);return(0,l.kt)("wrapper",(0,n.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("p",null,"Normally, schemas do not stay the same over a long period of time. Instead, they undergo evolutions to satisfy new needs. "),(0,l.kt)("p",null,"This chapter examines how Pulsar schema evolves and what Pulsar schema compatibility check strategies are."),(0,l.kt)("h2",{id:"schema-evolution"},"Schema evolution"),(0,l.kt)("p",null,"Pulsar schema is defined in a data structure called ",(0,l.kt)("inlineCode",{parentName:"p"},"SchemaInfo"),". "),(0,l.kt)("p",null,"Each ",(0,l.kt)("inlineCode",{parentName:"p"},"SchemaInfo")," stored with a topic has a version. The version is used to manage the schema changes happening within a topic. "),(0,l.kt)("p",null,"The message produced with ",(0,l.kt)("inlineCode",{parentName:"p"},"SchemaInfo")," is tagged with a schema version. When a message is consumed by a Pulsar client, the Pulsar client can use the schema version to retrieve the corresponding ",(0,l.kt)("inlineCode",{parentName:"p"},"SchemaInfo")," and use the correct schema information to deserialize data."),(0,l.kt)("h3",{id:"what-is-schema-evolution"},"What is schema evolution?"),(0,l.kt)("p",null,"Schemas store the details of attributes and types. To satisfy new business requirements,  you need to update schemas inevitably over time, which is called ",(0,l.kt)("strong",{parentName:"p"},"schema evolution"),". "),(0,l.kt)("p",null,"Any schema changes affect downstream consumers. Schema evolution ensures that the downstream consumers can seamlessly handle data encoded with both old schemas and new schemas. "),(0,l.kt)("h3",{id:"how-pulsar-schema-should-evolve"},"How Pulsar schema should evolve?"),(0,l.kt)("p",null,"The answer is Pulsar schema compatibility check strategy. It determines how schema compares old schemas with new schemas in topics."),(0,l.kt)("p",null,"For more information, see ",(0,l.kt)("a",{parentName:"p",href:"#schema-compatibility-check-strategy"},"Schema compatibility check strategy"),"."),(0,l.kt)("h3",{id:"how-does-pulsar-support-schema-evolution"},"How does Pulsar support schema evolution?"),(0,l.kt)("ol",null,(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"When a producer/consumer/reader connects to a broker, the broker deploys the schema compatibility checker configured by ",(0,l.kt)("inlineCode",{parentName:"p"},"schemaRegistryCompatibilityCheckers")," to enforce schema compatibility check. "),(0,l.kt)("p",{parentName:"li"},"The schema compatibility checker is one instance per schema type. "),(0,l.kt)("p",{parentName:"li"},"Currently, Avro and JSON have their own compatibility checkers, while all the other schema types share the default compatibility checker which disables schema evolution.")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"The producer/consumer/reader sends its client ",(0,l.kt)("inlineCode",{parentName:"p"},"SchemaInfo")," to the broker. ")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"The broker knows the schema type and locates the schema compatibility checker for that type. ")),(0,l.kt)("li",{parentName:"ol"},(0,l.kt)("p",{parentName:"li"},"The broker uses the checker to check if the ",(0,l.kt)("inlineCode",{parentName:"p"},"SchemaInfo")," is compatible with the latest schema of the topic by applying its compatibility check strategy. "),(0,l.kt)("p",{parentName:"li"},"Currently, the compatibility check strategy is configured at the namespace level and applied to all the topics within that namespace."))),(0,l.kt)("h2",{id:"schema-compatibility-check-strategy"},"Schema compatibility check strategy"),(0,l.kt)("p",null,"Pulsar has 8 schema compatibility check strategies, which are summarized in the following table."),(0,l.kt)("p",null,"Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:"),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Compatibility check strategy"),(0,l.kt)("th",{parentName:"tr",align:null},"Definition"),(0,l.kt)("th",{parentName:"tr",align:null},"Changes allowed"),(0,l.kt)("th",{parentName:"tr",align:null},"Check against which schema"),(0,l.kt)("th",{parentName:"tr",align:null},"Upgrade first"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_COMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"Disable schema compatibility check."),(0,l.kt)("td",{parentName:"tr",align:null},"All changes are allowed"),(0,l.kt)("td",{parentName:"tr",align:null},"All previous versions"),(0,l.kt)("td",{parentName:"tr",align:null},"Any order")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_INCOMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"Disable schema evolution."),(0,l.kt)("td",{parentName:"tr",align:null},"All changes are disabled"),(0,l.kt)("td",{parentName:"tr",align:null},"None"),(0,l.kt)("td",{parentName:"tr",align:null},"None")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the schema V3 can process data written by producers using the schema V3 or V2."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"Add optional fields "),(0,l.kt)("li",null,"Delete fields ")),(0,l.kt)("td",{parentName:"tr",align:null},"Latest version"),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the schema V3 can process data written by producers using the schema V3, V2 or V1."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"Add optional fields "),(0,l.kt)("li",null,"Delete fields ")),(0,l.kt)("td",{parentName:"tr",align:null},"All previous versions"),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the schema V3 or V2 can process data written by producers using the schema V3."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"Add fields "),(0,l.kt)("li",null,"Delete optional fields ")),(0,l.kt)("td",{parentName:"tr",align:null},"Latest version"),(0,l.kt)("td",{parentName:"tr",align:null},"Producers")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the schema V3, V2 or V1 can process data written by producers using the schema V3."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"Add fields "),(0,l.kt)("li",null,"Delete optional fields ")),(0,l.kt)("td",{parentName:"tr",align:null},"All previous versions"),(0,l.kt)("td",{parentName:"tr",align:null},"Producers")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FULL")),(0,l.kt)("td",{parentName:"tr",align:null},"Backward and forward compatible between the schema V3 and V2."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"Modify optional fields ")),(0,l.kt)("td",{parentName:"tr",align:null},"Latest version"),(0,l.kt)("td",{parentName:"tr",align:null},"Any order")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FULL_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Backward and forward compatible among the schema V3, V2, and V1."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"Modify optional fields ")),(0,l.kt)("td",{parentName:"tr",align:null},"All previous versions"),(0,l.kt)("td",{parentName:"tr",align:null},"Any order")))),(0,l.kt)("h3",{id:"always_compatible-and-always_incompatible"},"ALWAYS_COMPATIBLE and ALWAYS_INCOMPATIBLE"),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Compatibility check strategy"),(0,l.kt)("th",{parentName:"tr",align:null},"Definition"),(0,l.kt)("th",{parentName:"tr",align:null},"Note"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_COMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"Disable schema compatibility check."),(0,l.kt)("td",{parentName:"tr",align:null},"None")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_INCOMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"Disable schema evolution, that is, any schema change is rejected."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"For all schema types except Avro and JSON, the default schema compatibility check strategy is ",(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_INCOMPATIBLE"),". "),(0,l.kt)("li",null,"For Avro and JSON, the default schema compatibility check strategy is ",(0,l.kt)("inlineCode",{parentName:"td"},"FULL"),". "))))),(0,l.kt)("h4",{id:"example"},"Example"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Example  1"),(0,l.kt)("p",{parentName:"li"},"In some situations, an application needs to store events of several different types in the same Pulsar topic. "),(0,l.kt)("p",{parentName:"li"},"In particular, when developing a data model in an ",(0,l.kt)("inlineCode",{parentName:"p"},"Event Sourcing")," style, you might have several kinds of events that affect the state of an entity. "),(0,l.kt)("p",{parentName:"li"},"For example, for a user entity, there are ",(0,l.kt)("inlineCode",{parentName:"p"},"userCreated"),", ",(0,l.kt)("inlineCode",{parentName:"p"},"userAddressChanged")," and ",(0,l.kt)("inlineCode",{parentName:"p"},"userEnquiryReceived")," events. The application requires that those events are always read in the same order. "),(0,l.kt)("p",{parentName:"li"},"Consequently, those events need to go in the same Pulsar partition to maintain order. This application can use ",(0,l.kt)("inlineCode",{parentName:"p"},"ALWAYS_COMPATIBLE")," to allow different kinds of events co-exist in the same topic.")),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Example 2"),(0,l.kt)("p",{parentName:"li"},"Sometimes we also make incompatible changes. "),(0,l.kt)("p",{parentName:"li"},"For example, you are modifying a field type from ",(0,l.kt)("inlineCode",{parentName:"p"},"string")," to ",(0,l.kt)("inlineCode",{parentName:"p"},"int"),"."),(0,l.kt)("p",{parentName:"li"},"In this case, you need to:"),(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Upgrade all producers and consumers to the new schema versions at the same time.")),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Optionally, create a new topic and start migrating applications to use the new topic and the new schema, avoiding the need to handle two incompatible versions in the same topic."))))),(0,l.kt)("h3",{id:"backward-and-backward_transitive"},"BACKWARD and BACKWARD_TRANSITIVE"),(0,l.kt)("p",null,"Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:"),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Compatibility check strategy"),(0,l.kt)("th",{parentName:"tr",align:null},"Definition"),(0,l.kt)("th",{parentName:"tr",align:null},"Description"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the new schema can process data written by producers using the ",(0,l.kt)("strong",{parentName:"td"},"last schema"),"."),(0,l.kt)("td",{parentName:"tr",align:null},"The consumers using the schema V3 can process data written by producers using the schema V3 or V2.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the new schema can process data written by producers using ",(0,l.kt)("strong",{parentName:"td"},"all previous schemas"),"."),(0,l.kt)("td",{parentName:"tr",align:null},"The consumers using the schema V3 can process data written by producers using the schema V3, V2, or V1.")))),(0,l.kt)("h4",{id:"example-1"},"Example"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Example 1"),(0,l.kt)("p",{parentName:"li"},"Remove a field."),(0,l.kt)("p",{parentName:"li"},"A consumer constructed to process events without one field can process events written with the old schema containing the field, and the consumer will ignore that field.")),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Example 2"),(0,l.kt)("p",{parentName:"li"},"You want to load all Pulsar data into a Hive data warehouse and run SQL queries against the data. "),(0,l.kt)("p",{parentName:"li"},"Same SQL queries must continue to work even the data is changed. To support it, you can evolve the schemas using the ",(0,l.kt)("inlineCode",{parentName:"p"},"BACKWARD")," strategy."))),(0,l.kt)("h3",{id:"forward-and-forward_transitive"},"FORWARD and FORWARD_TRANSITIVE"),(0,l.kt)("p",null,"Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:"),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Compatibility check strategy"),(0,l.kt)("th",{parentName:"tr",align:null},"Definition"),(0,l.kt)("th",{parentName:"tr",align:null},"Description"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the ",(0,l.kt)("strong",{parentName:"td"},"last schema")," can process data written by producers using a new schema, even though they may not be able to use the full capabilities of the new schema."),(0,l.kt)("td",{parentName:"tr",align:null},"The consumers using the schema V3 or V2 can process data written by producers using the schema V3.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using ",(0,l.kt)("strong",{parentName:"td"},"all previous schemas")," can process data written by producers using a new schema."),(0,l.kt)("td",{parentName:"tr",align:null},"The consumers using the schema V3, V2, or V1 can process data written by producers using the schema V3.")))),(0,l.kt)("h4",{id:"example-2"},"Example"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Example 1"),(0,l.kt)("p",{parentName:"li"},"Add a field."),(0,l.kt)("p",{parentName:"li"},"In most data formats, consumers written to process events without new fields can continue doing so even when they receive new events containing new fields.")),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Example 2"),(0,l.kt)("p",{parentName:"li"},"If a consumer has an application logic tied to a full version of a schema, the application logic may not be updated instantly when the schema evolves."),(0,l.kt)("p",{parentName:"li"},"In this case, you need to project data with a new schema onto an old schema that the application understands. "),(0,l.kt)("p",{parentName:"li"},"Consequently, you can evolve the schemas using the ",(0,l.kt)("inlineCode",{parentName:"p"},"FORWARD")," strategy to ensure that the old schema can process data encoded with the new schema."))),(0,l.kt)("h3",{id:"full-and-full_transitive"},"FULL and FULL_TRANSITIVE"),(0,l.kt)("p",null,"Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:"),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Compatibility check strategy"),(0,l.kt)("th",{parentName:"tr",align:null},"Definition"),(0,l.kt)("th",{parentName:"tr",align:null},"Description"),(0,l.kt)("th",{parentName:"tr",align:null},"Note"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FULL")),(0,l.kt)("td",{parentName:"tr",align:null},"Schemas are both backward and forward compatible, which means: Consumers using the last schema can process data written by producers using the new schema. AND Consumers using the new schema can process data written by producers using the last schema."),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the schema V3 can process data written by producers using the schema V3 or V2. AND Consumers using the schema V3 or V2 can process data written by producers using the schema V3."),(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,"For Avro and JSON, the default schema compatibility check strategy is ",(0,l.kt)("inlineCode",{parentName:"td"},"FULL"),". "),(0,l.kt)("li",null,"For all schema types except Avro and JSON, the default schema compatibility check strategy is ",(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_INCOMPATIBLE"),". "))),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FULL_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"The new schema is backward and forward compatible with all previously registered schemas."),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers using the schema V3 can process data written by producers using the schema V3, V2 or V1. AND Consumers using the schema V3, V2 or V1 can process data written by producers using the schema V3."),(0,l.kt)("td",{parentName:"tr",align:null},"None")))),(0,l.kt)("h4",{id:"example-3"},"Example"),(0,l.kt)("p",null,"In some data formats, for example, Avro, you can define fields with default values. Consequently, adding or removing a field with a default value is a fully compatible change."),(0,l.kt)("h2",{id:"schema-verification"},"Schema verification"),(0,l.kt)("p",null,"When a producer or a consumer tries to connect to a topic, a broker performs some checks to verify a schema."),(0,l.kt)("h3",{id:"producer"},"Producer"),(0,l.kt)("p",null,"When a producer tries to connect to a topic (suppose ignore the schema auto creation), a broker does the following checks:"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"Check if the schema carried by the producer exists in the schema registry or not."),(0,l.kt)("ul",{parentName:"li"},(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"If the schema is already registered, then the producer is connected to a broker and produce messages with that schema.")),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("p",{parentName:"li"},"If the schema is not registered, then Pulsar verifies if the schema is allowed to be registered based on the configured compatibility check strategy."))))),(0,l.kt)("h3",{id:"consumer"},"Consumer"),(0,l.kt)("p",null,"When a consumer tries to connect to a topic, a broker checks if a carried schema is compatible with a registered schema based on the configured schema compatibility check strategy."),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Compatibility check strategy"),(0,l.kt)("th",{parentName:"tr",align:null},"Check logic"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_COMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"All pass")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_INCOMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"No pass")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD")),(0,l.kt)("td",{parentName:"tr",align:null},"Can read the last schema")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Can read all schemas")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD")),(0,l.kt)("td",{parentName:"tr",align:null},"Can read the last schema")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Can read the last schema")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FULL")),(0,l.kt)("td",{parentName:"tr",align:null},"Can read the last schema")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"FULL_TRANSITIVE")),(0,l.kt)("td",{parentName:"tr",align:null},"Can read all schemas")))),(0,l.kt)("h2",{id:"order-of-upgrading-clients"},"Order of upgrading clients"),(0,l.kt)("p",null,"The order of upgrading client applications is determined by the compatibility check strategy."),(0,l.kt)("p",null,"For example, the producers using schemas to write data to Pulsar and the consumers using schemas to read data from Pulsar. "),(0,l.kt)("table",null,(0,l.kt)("thead",{parentName:"table"},(0,l.kt)("tr",{parentName:"thead"},(0,l.kt)("th",{parentName:"tr",align:null},"Compatibility check strategy"),(0,l.kt)("th",{parentName:"tr",align:null},"Upgrade first"),(0,l.kt)("th",{parentName:"tr",align:null},"Description"))),(0,l.kt)("tbody",{parentName:"table"},(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_COMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"Any order"),(0,l.kt)("td",{parentName:"tr",align:null},"The compatibility check is disabled. Consequently, you can upgrade the producers and consumers in ",(0,l.kt)("strong",{parentName:"td"},"any order"),".")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("inlineCode",{parentName:"td"},"ALWAYS_INCOMPATIBLE")),(0,l.kt)("td",{parentName:"tr",align:null},"None"),(0,l.kt)("td",{parentName:"tr",align:null},"The schema evolution is disabled.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD")," "),(0,l.kt)("li",null,(0,l.kt)("inlineCode",{parentName:"td"},"BACKWARD_TRANSITIVE")," ")),(0,l.kt)("td",{parentName:"tr",align:null},"Consumers"),(0,l.kt)("td",{parentName:"tr",align:null},"There is no guarantee that consumers using the old schema can read data produced using the new schema. Consequently, ",(0,l.kt)("strong",{parentName:"td"},"upgrade all consumers first"),", and then start producing new data.")),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD")," "),(0,l.kt)("li",null,(0,l.kt)("inlineCode",{parentName:"td"},"FORWARD_TRANSITIVE")," ")),(0,l.kt)("td",{parentName:"tr",align:null},"Producers"),(0,l.kt)("td",{parentName:"tr",align:null},"There is no guarantee that consumers using the new schema can read data produced using the old schema. Consequently, ",(0,l.kt)("strong",{parentName:"td"},"upgrade all producers first"),(0,l.kt)("li",null,"to use the new schema and ensure that the data already produced using the old schemas are not available to consumers, and then upgrade the consumers. "))),(0,l.kt)("tr",{parentName:"tbody"},(0,l.kt)("td",{parentName:"tr",align:null},(0,l.kt)("li",null,(0,l.kt)("inlineCode",{parentName:"td"},"FULL")," "),(0,l.kt)("li",null,(0,l.kt)("inlineCode",{parentName:"td"},"FULL_TRANSITIVE")," ")),(0,l.kt)("td",{parentName:"tr",align:null},"Any order"),(0,l.kt)("td",{parentName:"tr",align:null},"There is no guarantee that consumers using the old schema can read data produced using the new schema and consumers using the new schema can read data produced using the old schema. Consequently, you can upgrade the producers and consumers in ",(0,l.kt)("strong",{parentName:"td"},"any order"),".")))))}c.isMDXComponent=!0}}]);
// const lightCodeTheme = require("prism-react-renderer/themes/github");
// const darkCodeTheme = require("prism-react-renderer/themes/dracula");

const linkifyRegex = require("./plugins/remark-linkify-regex");

const url = "https://pulsar.apache.org";
const javadocUrl = url + "/api";
const restApiUrl = url + "/admin-rest-api";
const functionsApiUrl = url + "/functions-rest-api";
const sourceApiUrl = url + "/source-rest-api";
const sinkApiUrl = url + "/sink-rest-api";
const packagesApiUrl = url + "/packages-rest-api";
const githubUrl = "https://github.com/apache/pulsar";
const baseUrl = "/";

const injectLinkParse = ([, prefix, , name, path]) => {
  if (prefix == "javadoc") {
    return {
      link: javadocUrl + path,
      text: name,
    };
  } else if (prefix == "github") {
    return {
      link: githubUrl + "/tree/master/" + path,
      text: name,
    };
  } else if (prefix == "rest") {
    return {
      link: restApiUrl + "#" + path,
      text: name,
    };
  } else if (prefix == "functions") {
    return {
      link: functionsApiUrl + "#" + path,
      text: name,
    };
  } else if (prefix == "source") {
    return {
      link: sourceApiUrl + "#" + path,
      text: name,
    };
  } else if (prefix == "sink") {
    return {
      link: sinkApiUrl + "#" + path,
      text: name,
    };
  } else if (prefix == "packages") {
    return {
      link: packagesApiUrl + "#" + path,
      text: name,
    };
  }

  return {
    link: path,
    text: name,
  };
};

const injectLinkParseForEndpoint = ([, info]) => {
  let [method, path, suffix] = info.split("|");
  if (!suffix) {
    suffix = path;
  }

  let restPath = path.split("/");
  const restApiVersion = restPath[2];
  const restApiType = restPath[3];
  let restBaseUrl = restApiUrl;
  if (restApiType == "functions") {
    restBaseUrl = functionsApiUrl;
  } else if (restApiType == "source") {
    restBaseUrl = sourceApiUrl;
  } else if (restApiType == "sink") {
    restBaseUrl = sinkApiUrl;
  }
  let restUrl = "";
  if (suffix.indexOf("?version") >= 0) {
    restUrl = suffix + "&apiVersion=" + restApiVersion;
  } else {
    restUrl = suffix + "version=master&apiVersion=" + restApiVersion;
  }
  return {
    text: method + " " + path,
    link: restBaseUrl + "#" + restUrl,
  };
};

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: "Apache Pulsar",
  tagline:
    "Apache Pulsar is a cloud-native, distributed messaging and streaming platform originally created at Yahoo! and now a top-level Apache Software Foundation project",
  url: "https://pulsar.apache.com",
  baseUrl: baseUrl,
  onBrokenLinks: "ignore",
  onBrokenMarkdownLinks: "ignore",
  favicon: "img/favicon.ico",
  organizationName: "apache",
  projectName: "pulsar",
  customFields: {
    githubUrl,
  },
  i18n: {
    defaultLocale: "en",
    locales: ["en", "zh"],
  },
  themeConfig: {
    navbar: {
      title: "",
      logo: {
        alt: "pulasr logo",
        src: "img/logo.svg",
      },
      items: [
        {
          type: 'dropdown',
          label: 'Get Started',
          position: 'right',
          items: [
            {
              type: "doc",
              docId: "concepts-overview",
              label: 'Pulsar Concepts'
            },
            {
              type: "doc",
              label: 'Quickstart',
              docId: "about",
            },
            {
              label: 'Ecosystem',
              to: 'ecosystem'
            }
            // ... more items
          ],
        },
        {
          type: "doc",
          docId: "standalone",
          position: "right",
          label: "Docs",
        },
        {
          type: 'dropdown',
          label: 'Community',
          position: 'right',
          items: [
            { 
              to: "community#welcome", 
              label: "Welcome" 
            },
            { 
              to: "community#discussions", 
              label: "Discussions"
            },
            { 
              to: "community#governance", 
              label: "Governance"
            },
            { 
              to: "community#community", 
              label: "Meet the Community"
            },
            { 
              to: "community#how-to-contribute", 
              label: "Contribute"
            },
            { 
              to: "https://github.com/apache/pulsar/wiki", 
              label: "Wiki"
            },
            { 
              to: "https://github.com/apache/pulsar/issues", 
              label: "Issue Tracking"
            },

          ]
        },
        {
          type: 'dropdown',
          label: 'Learn',
          position: 'right',
          items: [
            { 
              to: "blog", 
              label: "Blog" 
            },
            { 
              to: "case-studies", 
              label: "Case Studies"
            },
            { 
              to: "resources", 
              label: "Resources"
            },
            { 
              to: "events", 
              label: "Events"
            },

          ]
        },
        // {
        //   type: "localeDropdown",
        //   position: "left",
        // },
        {
          label: "Version",
          to: "docs",
          position: "right",
          items: [
            {
              label: "2.9.1",
              to: "docs/",
            },
            {
              label: "2.9.0",
              to: "docs/2.9.0/",
            },
            {
              label: "2.8.2",
              to: "docs/2.8.2/",
            },
            {
              label: "2.8.1",
              to: "docs/2.8.1/",
            },
            {
              label: "2.8.0",
              to: "docs/2.8.0/",
            },
            {
              label: "2.7.3",
              to: "docs/2.7.3/",
            },
            {
              label: "2.7.2",
              to: "docs/2.7.2/",
            },
            {
              label: "2.7.1",
              to: "docs/2.7.1/",
            },
            {
              label: "2.7.0",
              to: "docs/2.7.0/",
            },
            {
              label: "2.6.4",
              to: "docs/2.6.4/",
            },
            {
              label: "2.6.3",
              to: "docs/2.6.3/",
            },
            {
              label: "2.6.2",
              to: "docs/2.6.2/",
            },
            {
              label: "2.6.1",
              to: "docs/2.6.1/",
            },
            {
              label: "2.6.0",
              to: "docs/2.6.0/",
            },
            {
              label: "2.2.0",
              to: "docs/2.2.0/",
            },
          ],
        },
      ],
    },
    footer: {
      logo: {
        alt: 'Pulsar Logo',
        src: 'img/pulsar-white.svg',
        href: '/',
      },
      links: [
        {
          title: 'Apache Foundation.', //Column title
	          items: [
              { //Embedded HTML
	              html: `
	              <img src="/img/Apache_Feather_Logo.svg" alt="" width="20">
	              `,
	            },
	            {
	              label: "Foundation",
                href: "http://www.apache.org/",
	            },
	            {
	              label: "Events",
                href: "https://www.apache.org/events/current-event.html",
	            },
	            {
	              label: "License",
                href: "https://www.apache.org/licenses/",
	            },
	            {
	              label: "Thanks",
                href: "https://www.apache.org/foundation/thanks",
	            },
	            {
	              label: "Security",
                href: "https://www.apache.org/security",
	            },
	            {
	              label: "Sponsorship",
                href: "https://www.apache.org/foundation/sponsorship",
	            },
	          ],
        },
        {
          title: ' ', //Column title
	          items: [
	            { //Embedded HTML
	              html: `
	              <div><small><strong>Apache Pulsar is available under the <a href="/">Apache License, version 2.0.</a></strong></small></div>
                <div>Apache Pulsar is a distributed, open source pub-sub messaging and streaming platform for real-time workloads, managing hundreds of billions of events per day.</div>
	              `,
	            },
	          ],
        },
      ],
      copyright: `<p>Apache Pulsar is available under the Apache License, version 2.0.</p>
      <p>Inc.Copyright © ${new Date().getFullYear()} The Apache Software Foundation. All Rights Reserved. Apache, Apache Pulsar and the Apache feather logo are trademarks of The Apache Software Foundation.</p>`,
    },
    prism: {
      // theme: lightCodeTheme,
      // darkTheme: darkCodeTheme,
      theme: require("prism-react-renderer/themes/dracula"),
      additionalLanguages: ["powershell", "java", "go", "c", "cpp", "python"],
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          // Please change this to your repo.
          editUrl: `${githubUrl}/edit/master/site2/website-next`,
          remarkPlugins: [
            linkifyRegex(
              /{\@inject\:\s?(((?!endpoint)[^}])+):([^}]+):([^}]+)}/,
              injectLinkParse
            ),
            linkifyRegex(
              /{\@inject\:\s?endpoint\|([^}]+)}/,
              injectLinkParseForEndpoint
            ),
          ],
        },
        blog: {
          showReadingTime: true,
          blogTitle: 'Blog',
          // Please change this to your repo.
          editUrl: `${githubUrl}/edit/master/site2/website-next`,
          blogDescription: 'Featuring the latest news related to Apache Pulsar',
          postsPerPage: 10,
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],
  plugins: [
    [
      "@docusaurus/plugin-client-redirects",
      {
        fromExtensions: ["md"],
      },
    ],
    "./postcss-tailwind-loader",
  ],
  // clientModules: [
  //   require.resolve("./mySiteGlobalJs.js"),
  //   require.resolve("./mySiteGlobalCss.css"),
  // ],
};

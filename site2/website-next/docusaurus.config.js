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
  // i18n: {
  //   defaultLocale: "en",
  //   locales: ["en", "zh"],
  // },
  themeConfig: {
    navbar: {
      title: "",
      logo: {
        alt: "",
        src: "img/logo.svg",
      },
      items: [
        {
          type: "doc",
          docId: "standalone",
          position: "left",
          label: "Docs",
        },
        { to: "/download", label: "Download", position: "left" },
        {
          type: "doc",
          docId: "client-libraries",
          position: "left",
          label: "Clients",
        },
        {
          label: "REST APIs",
          position: "left",
          items: [
            {
              label: "Admin REST API",
              to: "/admin-rest-api",
            },
            {
              label: "Functions",
              to: "/functions-rest-api",
            },
            {
              label: "Sources",
              to: "/source-rest-api",
            },
            {
              label: "Sinks",
              to: "/sink-rest-api",
            },
            {
              label: "Packages",
              to: "/packages-rest-api",
            },
          ],
        },
        {
          label: "CLI",
          position: "left",
          items: [
            {
              label: "Pulsar Admin",
              to: "/pulsar-admin-cli",
            },
            {
              label: "Pulsar Client",
              to: "/pulsar-client-cli",
            },
            {
              label: "Pulsar Perf",
              to: "/pulsar-perf-cli",
            },
            {
              label: "Pulsar",
              to: "/pulsar-cli",
            },
          ],
        },
        { to: "/blog", label: "Blog", position: "left" },
        {
          label: "Community",
          position: "left",
          items: [
            {
              label: "Contact",
              to: "/:locale/contact",
            },
            {
              label: "Contributing",
              to: "/:locale/contributing",
            },
            {
              label: "Coding guide",
              to: "/:locale/coding-guide",
            },
            {
              label: "Events",
              to: "/:locale/events",
            },
            {
              label: "Twitter",
              href: "https://twitter.com/Apache_Pulsar",
            },
            {
              label: "Wiki",
              href: "https://github.com/apache/pulsar/wiki",
            },
            {
              label: "Issue tracking",
              href: "https://github.com/apache/pulsar/issues",
            },
            {
              label: "Pulsar Summit",
              href: "https://pulsar-summit.org/",
            },
            {
              label: "Resources",
              to: "/:locale/resources",
            },
            {
              label: "Team",
              to: "/:locale/team",
            },
            {
              label: "Powered By",
              to: "/:locale/powered-by",
            },
          ],
        },
        {
          label: "Apache",
          position: "left",
          items: [
            {
              label: "Foundation",
              href: "https://www.apache.org/",
            },
            {
              label: "License",
              href: "https://www.apache.org/licenses/",
            },
            {
              label: "Sponsorship",
              href: "https://www.apache.org/foundation/sponsorship.html",
            },
            {
              label: "Thanks",
              href: "https://www.apache.org/foundation/thanks.html",
            },
            {
              label: "Security",
              href: "https://www.apache.org/security",
            },
          ],
        },
        {
          href: "https://github.com/apache/pulsar",
          label: "GitHub",
          position: "right",
        },
        // {
        //   type: "localeDropdown",
        //   position: "left",
        // },
        // {
        //   label: "Version",
        //   to: "docs",
        //   position: "right",
        //   items: [
        //     {
        //       label: "2.9.1",
        //       to: "docs/2.9.1/",
        //     },
        //     {
        //       label: "2.9.0",
        //       to: "docs/2.9.0/",
        //     },
        //     {
        //       label: "2.8.2",
        //       to: "docs/2.8.2/",
        //     },
        //     {
        //       label: "2.8.1",
        //       to: "docs/2.8.1/",
        //     },
        //     {
        //       label: "2.8.0",
        //       to: "docs/2.8.0/",
        //     },
        //     {
        //       label: "2.7.3",
        //       to: "docs/2.7.3/",
        //     },
        //     {
        //       label: "2.7.2",
        //       to: "docs/2.7.2/",
        //     },
        //     {
        //       label: "2.7.1",
        //       to: "docs/2.7.1/",
        //     },
        //     {
        //       label: "2.7.0",
        //       to: "docs/2.7.0/",
        //     },
        //     {
        //       label: "2.6.4",
        //       to: "docs/2.6.4/",
        //     },
        //     {
        //       label: "2.6.3",
        //       to: "docs/2.6.3/",
        //     },
        //     {
        //       label: "2.6.2",
        //       to: "docs/2.6.2/",
        //     },
        //     {
        //       label: "2.6.1",
        //       to: "docs/2.6.1/",
        //     },
        //     {
        //       label: "2.6.0",
        //       to: "docs/2.6.0/",
        //     },
        //     {
        //       label: "2.5.2",
        //       to: "docs/2.5.2/",
        //     },
        //     {
        //       label: "2.5.1",
        //       to: "docs/2.5.1/",
        //     },
        //     {
        //       label: "2.5.0",
        //       to: "docs/2.5.0/",
        //     },
        //     {
        //       label: "2.4.2",
        //       to: "docs/2.4.2/",
        //     },
        //     {
        //       label: "2.4.1",
        //       to: "docs/2.4.1/",
        //     },
        //     {
        //       label: "2.4.0",
        //       to: "docs/2.4.0/",
        //     },
        //     {
        //       label: "2.3.2",
        //       to: "docs/2.3.2/",
        //     },
        //     {
        //       label: "2.3.1",
        //       to: "docs/2.3.1/",
        //     },
        //     {
        //       label: "2.3.0",
        //       to: "docs/2.3.0/",
        //     },
        //     {
        //       label: "2.2.1",
        //       to: "docs/2.2.1/",
        //     },
        //     {
        //       label: "2.2.0",
        //       to: "docs/2.2.0/",
        //     },
        //     {
        //       label: "2.1.1-incubating",
        //       to: "docs/2.1.1-incubating/",
        //     },
        //   ],
        // },
      ],
    },
    // scripts: [
    //   "https://buttons.github.io/buttons.js",
    //   "https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.0/clipboard.min.js",
    //   `${baseUrl}js/custom.js`,
    // ],
    footer: {
      style: "dark",
      copyright: `Inc.Copyright © ${new Date().getFullYear()} The Apache Software Foundation. All Rights Reserved. Apache, Apache Pulsar and the Apache feather logo are trademarks of The Apache Software Foundation.`,
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
          // Please change this to your repo.
          editUrl: `${githubUrl}/edit/master/site2/website-next`,
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

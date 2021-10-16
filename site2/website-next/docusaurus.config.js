const lightCodeTheme = require("prism-react-renderer/themes/github");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");

const linkifyRegex = require("./plugins/remark-linkify-regex");

const url = "https://pulsar.incubator.apache.org";
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
        { to: "/blog", label: "Blog", position: "left" },
        {
          href: "https://github.com/apache/pulsar",
          label: "GitHub",
          position: "right",
        },
        {
          label: "Version",
          to: "docs",
          position: "right",
          items: [
            {
              label: "2.8.0",
              to: "docs/",
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
              label: "2.2.0",
              to: "docs/2.2.0/",
            },
          ],
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "More",
          items: [
            {
              label: "Docs",
              to: "/docs",
            },
            {
              label: "Blog",
              to: "/blog",
            },
            {
              label: "GitHub",
              href: "https://github.com/apache/pulsar",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Apache Pulsar, Inc.`,
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
  ],
};

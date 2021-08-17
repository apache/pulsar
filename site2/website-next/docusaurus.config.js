const lightCodeTheme = require("prism-react-renderer/themes/github");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");
const linkifyRegex = require("./remark-linkify-regex");

const url = "https://pulsar.incubator.apache.org";
const javadocUrl = url + "/api";
const restApiUrl = url + "/admin-rest-api";
const functionsApiUrl = url + "/functions-rest-api";
const sourceApiUrl = url + "/source-rest-api";
const sinkApiUrl = url + "/sink-rest-api";
const packagesApiUrl = url + "/packages-rest-api";
const githubUrl = "https://github.com/apache/incubator-pulsar";
const baseUrl = "/";

const injectLinkParse = ([, prefix, name, path]) => {
  console.log(prefix, name, path);

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
    // } else if (prefix == "endpoint") {
    //   // endpoint api: endpoint|<op>
    //   // {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/registerSource?version=[[pulsar:version_number]]}
    //   // text: POST /admin/v3/sources/:tenant/:namespace/:sourceName
    //   // link: https://pulsar.incubator.apache.org/admin-rest-api#operation/registerSource?version=2.8.0&apiVersion=v3
    //   const restPath = path.split("/");
    //   const restApiVersion = restPath[2];
    //   const restApiType = restPath[3];
    //   let restBaseUrl = restApiUrl;
    //   if (restApiType == "functions") {
    //     restBaseUrl = functionsApiUrl;
    //   } else if (restApiType == "source") {
    //     restBaseUrl = sourceApiUrl;
    //   } else if (restApiType == "sink") {
    //     restBaseUrl = sinkApiUrl;
    //   }
    //   const suffix = keyparts[keyparts.length - 1];
    //   restUrl = "";
    //   if (suffix.indexOf("?version") >= 0) {
    //     restUrl = keyparts[keyparts.length - 1] + "&apiVersion=" + restApiVersion;
    //   } else {
    //     restUrl =
    //       keyparts[keyparts.length - 1] +
    //       "version=master&apiVersion=" +
    //       restApiVersion;
    //   }
    //   return renderEndpoint(
    //     initializedPlugin,
    //     restBaseUrl + "#",
    //     keyparts,
    //     restUrl
    //   );
  }

  return {
    link: path,
    text: name,
  };
};

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: "My Site",
  tagline: "Dinosaurs are cool",
  url: "https://your-docusaurus-test-site.com",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",
  favicon: "img/favicon.ico",
  organizationName: "facebook", // Usually your GitHub org/user name.
  projectName: "docusaurus", // Usually your repo name.
  themeConfig: {
    navbar: {
      title: "My Site",
      logo: {
        alt: "My Site Logo",
        src: "img/logo.svg",
      },
      items: [
        {
          type: "doc",
          docId: "intro",
          position: "left",
          label: "Tutorial",
        },
        { to: "/blog", label: "Blog", position: "left" },
        {
          href: "https://github.com/facebook/docusaurus",
          label: "GitHub",
          position: "right",
        },
        {
          label: "Version",
          to: "docs",
          position: "right",
          items: [
            {
              label: "1.1.1",
              to: "docs/",
              // activeBaseRegex:
              //   "docs/(?!2.1.0-incubating|2.1.1-incubating|2.2.0|2.2.1|2.3.0|2.3.1|2.3.2|2.4.0|2.4.1|2.4.2|2.5.0|2.5.1|2.5.2|2.6.0|2.6.1|2.6.2|2.6.3|2.6.4|2.7.0|2.7.1|2.7.2|2.7.3|2.8.0|next)",
            },
            {
              label: "1.1.0",
              to: "docs/1.1.0/",
            },
          ],
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            {
              label: "Tutorial",
              to: "/docs/intro",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Stack Overflow",
              href: "https://stackoverflow.com/questions/tagged/docusaurus",
            },
            {
              label: "Discord",
              href: "https://discordapp.com/invite/docusaurus",
            },
            {
              label: "Twitter",
              href: "https://twitter.com/docusaurus",
            },
          ],
        },
        {
          title: "More",
          items: [
            {
              label: "Blog",
              to: "/blog",
            },
            {
              label: "GitHub",
              href: "https://github.com/facebook/docusaurus",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} My Project, Inc. Built with Docusaurus.`,
    },
    prism: {
      theme: lightCodeTheme,
      darkTheme: darkCodeTheme,
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          // Please change this to your repo.
          editUrl:
            "https://github.com/facebook/docusaurus/edit/master/website/",
          remarkPlugins: [
            linkifyRegex(
              /\{\@inject\:\s?((?!endpoint).)+:(.+):(.+)\}/,
              injectLinkParse
            ),
          ],
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          editUrl:
            "https://github.com/facebook/docusaurus/edit/master/website/blog/",
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],
  // plugins: [createVariableInjectionPlugin(siteVariables)],
};

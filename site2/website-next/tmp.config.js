module.exports={
  "title": "Apache Pulsar",
  "tagline": "",
  "url": "https://pulsar.incubator.apache.org",
  "baseUrl": "/",
  "organizationName": "apache",
  "projectName": "pulsar",
  "scripts": [
    "https://buttons.github.io/buttons.js",
    "https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.0/clipboard.min.js",
    "/js/custom.js"
  ],
  "stylesheets": [
    "/css/code-blocks-buttons.css"
  ],
  "favicon": "img/pulsar.ico",
  "customFields": {
    "disableTitleTagline": true,
    "users": [],
    "translationRecruitingLink": "https://crowdin.com/project/apache-pulsar",
    "disableHeaderTitle": true,
    "githubUrl": "https://github.com/apache/incubator-pulsar",
    "projectDescription": "\n    Apache Pulsar is a cloud-native, distributed messaging and streaming platform originally\n    created at Yahoo! and now a top-level Apache Software Foundation project\n  ",
    "markdownPlugins": [
      null
    ]
  },
  "onBrokenLinks": "log",
  "onBrokenMarkdownLinks": "log",
  "presets": [
    [
      "@docusaurus/preset-classic",
      {
        "docs": {
          "homePageId": "standalone",
          "showLastUpdateAuthor": true,
          "showLastUpdateTime": true,
          "editUrl": "https://github.com/apache/incubator-pulsar/edit/master/site2/docs/",
          "path": "../docs",
          "sidebarPath": "../website/sidebars.json"
        },
        "blog": {
          "path": "blog"
        },
        "theme": {
          "customCss": "../src/css/customTheme.css"
        }
      }
    ]
  ],
  "plugins": [],
  "themeConfig": {
    "navbar": {
      "title": "Apache Pulsar",
      "logo": {
        "src": "img/pulsar.svg"
      },
      "items": [
        {
          "to": "docs/",
          "label": "Docs",
          "position": "left"
        },
        {
          "to": "/download",
          "label": "Download",
          "position": "left"
        },
        {
          "to": "docs/client-libraries",
          "label": "Clients",
          "position": "left"
        },
        {
          "href": "#restapis",
          "label": "REST APIs",
          "position": "left"
        },
        {
          "href": "#cli",
          "label": "Cli",
          "position": "left"
        },
        {
          "href": "#community",
          "label": "Community",
          "position": "left"
        },
        {
          "href": "#apache",
          "label": "Apache",
          "position": "left"
        },
        {
          "label": "Version",
          "to": "docs",
          "position": "right",
          "items": [
            {
              "label": "2.8.0",
              "to": "docs/",
              "activeBaseRegex": "docs/(?!2.1.0-incubating|2.1.1-incubating|2.2.0|2.2.1|2.3.0|2.3.1|2.3.2|2.4.0|2.4.1|2.4.2|2.5.0|2.5.1|2.5.2|2.6.0|2.6.1|2.6.2|2.6.3|2.6.4|2.7.0|2.7.1|2.7.2|2.7.3|2.8.0|next)"
            },
            {
              "label": "2.7.3",
              "to": "docs/2.7.3/"
            },
            {
              "label": "2.7.2",
              "to": "docs/2.7.2/"
            },
            {
              "label": "2.7.1",
              "to": "docs/2.7.1/"
            },
            {
              "label": "2.7.0",
              "to": "docs/2.7.0/"
            },
            {
              "label": "2.6.4",
              "to": "docs/2.6.4/"
            },
            {
              "label": "2.6.3",
              "to": "docs/2.6.3/"
            },
            {
              "label": "2.6.2",
              "to": "docs/2.6.2/"
            },
            {
              "label": "2.6.1",
              "to": "docs/2.6.1/"
            },
            {
              "label": "2.6.0",
              "to": "docs/2.6.0/"
            },
            {
              "label": "2.5.2",
              "to": "docs/2.5.2/"
            },
            {
              "label": "2.5.1",
              "to": "docs/2.5.1/"
            },
            {
              "label": "2.5.0",
              "to": "docs/2.5.0/"
            },
            {
              "label": "2.4.2",
              "to": "docs/2.4.2/"
            },
            {
              "label": "2.4.1",
              "to": "docs/2.4.1/"
            },
            {
              "label": "2.4.0",
              "to": "docs/2.4.0/"
            },
            {
              "label": "2.3.2",
              "to": "docs/2.3.2/"
            },
            {
              "label": "2.3.1",
              "to": "docs/2.3.1/"
            },
            {
              "label": "2.3.0",
              "to": "docs/2.3.0/"
            },
            {
              "label": "2.2.1",
              "to": "docs/2.2.1/"
            },
            {
              "label": "2.2.0",
              "to": "docs/2.2.0/"
            },
            {
              "label": "2.1.1-incubating",
              "to": "docs/2.1.1-incubating/"
            },
            {
              "label": "2.1.0-incubating",
              "to": "docs/2.1.0-incubating/"
            },
            {
              "label": "Master/Unreleased",
              "to": "docs/next/",
              "activeBaseRegex": "docs/next/(?!support|team|resources)"
            }
          ]
        }
      ]
    },
    "footer": {
      "links": [
        {
          "title": "Community",
          "items": [
            {
              "label": "Twitter",
              "to": "https://twitter.com/apache_pulsar"
            }
          ]
        }
      ],
      "copyright": "Copyright © 2021 The Apache Software Foundation. All Rights Reserved. Apache, Apache Pulsar and the Apache feather logo are trademarks of The Apache Software Foundation.",
      "logo": {
        "src": "img/pulsar.svg"
      }
    },
    "algolia": {
      "apiKey": "d226a455cecdd4bc18a554c1b47e5b52",
      "indexName": "apache_pulsar",
      "algoliaOptions": {
        "facetFilters": [
          "language:LANGUAGE",
          "version:VERSION"
        ]
      }
    },
    "gtag": {
      "trackingID": "UA-102219959-1"
    }
  }
}
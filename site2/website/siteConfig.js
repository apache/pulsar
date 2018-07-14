/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config.html for all the possible
// site configuration options.

const {Plugin: Embed} = require('remarkable-embed');


// Our custom remarkable plugin factory.
const createVariableInjectionPlugin = variables => {
  // `let` binding used to initialize the `Embed` plugin only once for efficiency.
  // See `if` statement below.
  let initializedPlugin;

  const embed = new Embed();
  embed.register({
    // Call the render method to process the corresponding variable with
    // the passed Remarkable instance.
    // -> the Markdown markup in the variable will be converted to HTML.
    inject: (key) => {
      console.log("key", key)
      console.log(initializedPlugin)
      return initializedPlugin.render(variables[key])
    }
  });

  return (md, options) => {
    if (!initializedPlugin) {
      initializedPlugin = {
        render: md.render.bind(md),
        hook: embed.hook(md, options)
      };
    }

    //console.log(md)
    //console.log(options)

    return initializedPlugin.hook;
  };
};






const siteVariables = {
  scalar: 'https://example.com',
  // Since the variables are processed by Docusaurus's Markdown converter,
  // this will become a nice syntax-highlighted code block.
  markdown: [
    '```javascript',
    'const highlighted = true;',
    '```',
  ].join('\n'),
  // We can use HTML directly too as HTML is valid Markdown.
  html: [
    '<details>',
    '  <summary>More details</summary>',
    '  <pre>Some details</pre>',
    '</details>'
  ].join('\n')
};


/* List of projects/orgs using your project for the users page */
const users = [
  {
    caption: 'User1',
    // You will need to prepend the image path with your baseUrl
    // if it is not '/', like: '/test-site/img/docusaurus.svg'.
    image: '/img/docusaurus.svg',
    infoLink: 'https://www.facebook.com',
    pinned: true,
  },
];


const GITHUB_URL = "https://github.com/apache/incubator-pulsar";

const siteConfig = {
  title: '' /* title for your website */,
  tagline: 'Apache Pulsar is an open-source distributed pub-sub messaging system originally created at Yahoo and now part of the Apache Software Foundation',
  url: 'https://your-docusaurus-test-site.com' /* your website url */,
  baseUrl: '/' /* base url for your project */,
  // For github.io type URLs, you would set the url and baseUrl like:
  //   url: 'https://facebook.github.io',
  //   baseUrl: '/test-site/',

  editUrl: `${GITHUB_URL}/blob/master/site2/docs/`,

  // Used for publishing and more
  projectName: 'ApachePulsar',
  organizationName: 'pulsar',
  // For top-level user or org sites, the organization is still the same.
  // e.g., for the https://JoelMarcey.github.io site, it would be set like...
  //   organizationName: 'JoelMarcey'

  // For no header links in the top nav bar -> headerLinks: [],
  headerLinks: [
    {doc: 'standalone', label: 'Documentation'},
    
    {page: 'download', label: 'Download'},
    {doc: 'client-libraries', label: 'Client libraries'},
    //{page: 'help', label: 'Help'},
    //{blog: true, label: 'Blog'},
    { search: true },
    // Determines language drop down position among links
    { languages: true }
  ],

  // If you have users set above, you add it here:
  users,

  /* path to images for header/footer */
  headerIcon: 'img/pulsar.svg',
  footerIcon: 'img/pulsar.svg',
  //footerIcon: 'img/docusaurus.svg',
  favicon: 'img/favicon.png',

  /* colors for website */
  colors: {
    //primaryColor: '#2E8555',
    //#188fff
    primaryColor: '#188fff',
    //primaryColor: '#fff',
    secondaryColor: '#205C3B',
  },

  /* custom fonts for website */
  /*fonts: {
    myFont: [
      "Times New Roman",
      "Serif"
    ],
    myOtherFont: [
      "-apple-system",
      "system-ui"
    ]
  },*/

  // This copyright info is used in /core/Footer.js and blog rss/atom feeds.
  copyright:
    'Copyright © ' +
    new Date().getFullYear() +
    ' The Apache Software Foundation. All Rights Reserved.' + 
    ' Apache, Apache Pulsar and the Apache feather logo are trademarks of The Apache Software Foundation.',

  highlight: {
    // Highlight.js theme to use for syntax highlighting in code blocks
    theme: 'atom-one-dark',
  },

  // Add custom scripts here that would be placed in <script> tags
  scripts: ['https://buttons.github.io/buttons.js'],

  /* On page navigation for the current documentation page */
  onPageNav: 'separate',

  /* Open Graph and Twitter card images */
  ogImage: 'img/docusaurus.png',
  twitterImage: 'img/docusaurus.png',

  // You may provide arbitrary config keys to be used as needed by your
  // template. For example, if you need your repo's URL...
  //   repoUrl: 'https://github.com/facebook/test-site',

  disableHeaderTitle: true,

  scrollToTop: true,
  scrollToTopOptions: {
    zIndex: 100,
  },

  githubUrl: 'https://github.com/apache/incubator-pulsar',
  archiveRootUrl: 'http://archive.apache.org/dist/incubator/pulsar',

  markdownPlugins: [
    createVariableInjectionPlugin(siteVariables),
    function foo(md) {
      md.renderer.rules.fence_custom.foo = function(
        tokens,
        idx,
        options,
        env,
        instance
      ) {
        console.log("calling custom function...")
        console.log("env:")
        console.log(options);
        console.log(env);
        //console.log(instance);
        console.log(idx);
        console.log(tokens.params);
        //console.log(process.argv)
        //console.log(process.cwd())
        //console.log(process)

        return '<div class="foo">bar</div>';
      };
    },
  ],
};

module.exports = siteConfig;

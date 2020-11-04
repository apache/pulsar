/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

/*
class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + (language ? language + '/' : '') + doc;
  }

  render() {
    const currentYear = new Date().getFullYear();
    return (
      <footer className="nav-footer" id="footer">
        <section className="sitemap">
          <a href={this.props.config.baseUrl} className="nav-home">
            {this.props.config.footerIcon && (
              <img
                src={this.props.config.baseUrl + this.props.config.footerIcon}
                alt={this.props.config.title}
                width="66"
                height="58"
              />
            )}
          </a>
          <div>
            <h5>Docs</h5>
            <a href={this.docUrl('doc1.html', this.props.language)}>
              Getting Started (or other categories)
            </a>
            <a href={this.docUrl('doc2.html', this.props.language)}>
              Guides (or other categories)
            </a>
            <a href={this.docUrl('doc3.html', this.props.language)}>
              API Reference (or other categories)
            </a>
          </div>
          <div>
            <h5>Community</h5>
            <a href={this.pageUrl('users.html', this.props.language)}>
              User Showcase
            </a>
            <a
              href="http://stackoverflow.com/questions/tagged/"
              target="_blank"
              rel="noreferrer noopener">
              Stack Overflow
            </a>
            <a href="https://discordapp.com/">Project Chat</a>
            <a
              href="https://twitter.com/"
              target="_blank"
              rel="noreferrer noopener">
              Twitter
            </a>
          </div>
          <div>
            <h5>More</h5>
            <a href={this.props.config.baseUrl + 'blog'}>Blog</a>
            <a href="https://github.com/">GitHub</a>
            <a
              className="github-button"
              href={this.props.config.repoUrl}
              data-icon="octicon-star"
              data-count-href="/facebook/docusaurus/stargazers"
              data-show-count={true}
              data-count-aria-label="# stargazers on GitHub"
              aria-label="Star this project on GitHub">
              Star
            </a>
          </div>
        </section>

        <a
          href="https://code.facebook.com/projects/"
          target="_blank"
          rel="noreferrer noopener"
          className="fbOpenSource">
          <img
            src={this.props.config.baseUrl + 'img/oss_logo.png'}
            alt="Facebook Open Source"
            width="170"
            height="45"
          />
        </a>
        <section className="copyright">{this.props.config.copyright}</section>
      </footer>
    );
  }
}
*/

class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + (language ? language + '/' : '') + doc;
  }

  render() {
    const currentYear = new Date().getFullYear();

    const contactUrl = this.pageUrl('contact', this.props.language)
    const eventsUrl = this.pageUrl('events', this.props.language)
    const twitterUrl = 'https://twitter.com/Apache_Pulsar'
    const wikiUrl = 'https://github.com/apache/pulsar/wiki'
    const issuesUrl = 'https://github.com/apache/pulsar/issues'
    const summitUrl = 'https://pulsar-summit.org/'
    const resourcesUrl = this.pageUrl('resources', this.props.language)
    const teamUrl = this.pageUrl('team', this.props.language)
    const poweredByUrl = this.pageUrl('powered-by', this.props.language)
    const contributingUrl = this.pageUrl('contributing', this.props.language)
    const codingUrl = this.pageUrl('coding-guide', this.props.language)

    const communityMenuJs = `
      const community = document.querySelector("a[href='#community']").parentNode;
      const communityMenu =
        '<li>' +
        '<a id="community-menu" href="#">Community <span style="font-size: 0.75em">&nbsp;▼</span></a>' +
        '<div id="community-dropdown" class="hide">' +
          '<ul id="community-dropdown-items">' +
            '<li><a href="${contactUrl}">Contact</a></li>' +
            '<li><a href="${contributingUrl}">Contributing</a></li>' +
            '<li><a href="${codingUrl}">Coding guide</a></li>' +
            '<li><a href="${eventsUrl}">Events</a></li>' +
            '<li><a href="${twitterUrl}" target="_blank">Twitter &#x2750</a></li>' +
            '<li><a href="${wikiUrl}" target="_blank">Wiki &#x2750</a></li>' +
            '<li><a href="${issuesUrl}" target="_blank">Issue tracking &#x2750</a></li>' +
            '<li><a href="${summitUrl}" target="_blank">Pulsar Summit &#x2750</a></li>' +
            '<li>&nbsp;</li>' +
            '<li><a href="${resourcesUrl}">Resources</a></li>' +
            '<li><a href="${teamUrl}">Team</a></li>' +
            '<li><a href="${poweredByUrl}">Powered By</a></li>' +
          '</ul>' +
        '</div>' +
        '</li>';

      community.innerHTML = communityMenu;

      const communityMenuItem = document.getElementById("community-menu");
      const communityDropDown = document.getElementById("community-dropdown");
      communityMenuItem.addEventListener("click", function(event) {
        event.preventDefault();

        if (communityDropDown.className == 'hide') {
          communityDropDown.className = 'visible';
        } else {
          communityDropDown.className = 'hide';
        }
      });
    `

    return (
      <footer className="nav-footer" id="footer">
        <section className="copyright">{this.props.config.copyright}</section>
        <span>
        <script dangerouslySetInnerHTML={{__html: communityMenuJs }} />
        </span>
      </footer>
    );
  }
}


module.exports = Footer;

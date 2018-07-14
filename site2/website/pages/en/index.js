/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const siteConfig = require(process.cwd() + '/siteConfig.js');

function imgUrl(img) {
  return siteConfig.baseUrl + 'img/' + img;
}

function docUrl(doc, language) {
  return siteConfig.baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
}

function pageUrl(page, language) {
  return siteConfig.baseUrl + (language ? language + '/' : '') + page;
}

function githubUrl() {
  return siteConfig.githubUrl;
}


class Button extends React.Component {
  render() {
    return (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={this.props.href} target={this.props.target}>
          {this.props.children}
        </a>
      </div>
    );
  }
}

Button.defaultProps = {
  target: '_self',
};

const SplashContainer = props => (
  <div className="homeContainer">
    <div className="homeSplashFade" style={{marginTop: '5rem'}}>
      <div className="wrapper homeWrapper">{props.children}</div>
    </div>
  </div>
);

const Logo = props => (
  <div className="" style={{width: '500px', alignItems: 'center', margin: 'auto'}}>
    <img src={props.img_src} />
  </div>
);

const ProjectTitle = props => (
  <h2 className="projectTitle" style={{maxWidth:'1024px', margin: 'auto'}}>
    {siteConfig.title}
    <small style={{color: 'black', fontSize: '2.0rem'}}>{siteConfig.tagline}</small>
  </h2>
);

const PromoSection = props => (
  <div className="section promoSection">
    <div className="promoRow">
      <div className="pluginRowBlock">{props.children}</div>
    </div>
  </div>
);

class HomeSplash extends React.Component {
  render() {
    let language = this.props.language || '';
    return (
      <SplashContainer>
        <Logo img_src={imgUrl('pulsar.svg')} />
        <div className="inner">
          <ProjectTitle />
          <PromoSection>
            <Button href={docUrl('standalone', language)}>Read the docs</Button>
            <Button href={githubUrl()}>GitHub</Button>
          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

const Block = props => (
  <Container
    padding={['bottom']}
    id={props.id}
    background={props.background}>
    <GridBlock align="center" contents={props.children} layout={props.layout} />
  </Container>
);


const features = {
  row1: [
    {
      content: 'Easily deploy lightweight compute logic using developer-friendly APIs without needing to run your own stream processing engine',
      title: '[Pulsar Functions]()',
    },
    {
      content: 'Pulsar has run in production at Yahoo scale for over 3 years, with millions of messages per second across millions of topics',
      title: '[Proven in production](getting-started-concepts-and-architecture.md)',
    },
    {
      content: 'Seamlessly expand capacity to hundreds of nodes',
      title: '[Horizontally scalable]()',
    }
  ],
  row2: [
    {
      content: 'Designed for low publish latency (< 5ms) at scale with strong durabilty guarantees',
      title: 'Low latency with durability',
    },
    {
      content: 'Designed for configurable replication between data centers across multiple geographic regions',
      title: 'Geo-replication',
    },
    {
      content: 'Built from the ground up as a multi-tenant system. Supports Isolation, Authentication, Authorization and Quotas',
      title: 'Multi-tenancy',
    }
  ],
  row3: [
    {
      content: `Persistent message storage based on Apache BookKeeper. Provides IO-level isolation between write and read operations`,
      title: 'Persistent storage',
    },
    {
      content: 'Flexible messaging models with high-level APIs for Java, C++, Python and GO',
      title: 'Client libraries',
    },
    {
      content: 'REST Admin API for provisioning, administration, tools and monitoring. Deploy on bare metal or Kubernetes.',
      title: 'Operability',
    }
  ]
};

const KeyFeautresGrid = props => (
  <Container
    padding={['bottom']}
    id={props.id}
    background={props.background}>
    <GridBlock align="center" contents={props.features.row1} layout="threeColumn" />
    <GridBlock align="center" contents={props.features.row2} layout="threeColumn" />
    <GridBlock align="center" contents={props.features.row3} layout="threeColumn" />
  </Container>
);

const Features = props => (
  <Block layout="threeColumn">
    {[
      {
        content: 'This is the content of my feature',
        image: imgUrl('docusaurus.svg'),
        imageAlign: 'top',
        title: 'Feature One',
      },
      {
        content: 'The content of my second feature',
        image: imgUrl('docusaurus.svg'),
        imageAlign: 'top',
        title: 'Feature Two',
      },
      {
        content: 'This is the content of my feature',
        image: imgUrl('docusaurus.svg'),
        imageAlign: 'top',
        title: 'Feature One',
      },
    ]}
  </Block>
);


const ApacheBlock = prop => (
  <Container>
    <div className="Block" style={{textAlign: 'center'}}>
      <div className="" style={{alignItems: 'center', margin: 'auto'}}>
        <img src="/img/apache_incubator.png" />
      </div>
      <MarkdownBlock>
        Apache Pulsar is an effort undergoing incubation at The [Apache Software Foundation (ASF)]() 
        sponsored by the Apache Incubator PMC. Incubation is required of all newly accepted projects 
        until a further review indicates that the infrastructure, communications, and decision making 
        process have stabilized in a manner consistent with other successful ASF projects. 
        While incubation status is not necessarily a reflection of the completeness or stability of the code, 
        it does indicate that the project has yet to be fully endorsed by the ASF.
        Apache Pulsar (incubating) is available under the [Apache License, version 2.0]().
      </MarkdownBlock>
    </div>
  </Container>
);

const FeatureCallout = props => (
  <div
    className="Block paddingBottom wrapper"
    style={{textAlign: 'center'}}>
    <div className="" style={{alignItems: 'center', margin: 'auto'}}>
      <img src="/img/apache_incubator.png" />
    </div>
    <MarkdownBlock>
    Apache Pulsar is an effort undergoing incubation at The [Apache Software Foundation (ASF)]() sponsored by the Apache Incubator PMC. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
    Apache Pulsar (incubating) is available under the [Apache License, version 2.0]().
    </MarkdownBlock>
  </div>
);

const LearnHow = props => (
  <Block background="light">
    {[
      {
        content: 'Talk about learning how to use this',
        image: imgUrl('docusaurus.svg'),
        imageAlign: 'right',
        title: 'Learn How',
      },
    ]}
  </Block>
);

const TryOut = props => (
  <Block id="try">
    {[
      {
        content: 'Talk about trying this out',
        image: imgUrl('docusaurus.svg'),
        imageAlign: 'left',
        title: 'Try it Out',
      },
    ]}
  </Block>
);

const Description = props => (
  <Block background="dark">
    {[
      {
        content: 'This is another description of how this project is useful',
        image: imgUrl('docusaurus.svg'),
        imageAlign: 'right',
        title: 'Description',
      },
    ]}
  </Block>
);

const Showcase = props => {
  if ((siteConfig.users || []).length === 0) {
    return null;
  }
  const showcase = siteConfig.users
    .filter(user => {
      return user.pinned;
    })
    .map((user, i) => {
      return (
        <a href={user.infoLink} key={i}>
          <img src={user.image} alt={user.caption} title={user.caption} />
        </a>
      );
    });

  return (
    <div className="productShowcaseSection paddingBottom">
      <h2>{"Who's Using This?"}</h2>
      <p>This project is used by all these people</p>
      <div className="logos">{showcase}</div>
      <div className="more-users">
        <a className="button" href={pageUrl('users.html', props.language)}>
          More {siteConfig.title} Users
        </a>
      </div>
    </div>
  );
};

class Index extends React.Component {
  render() {
    let language = this.props.language || '';

    return (
      <div>
        <HomeSplash language={language} />
        <div className="mainContainer">
          <KeyFeautresGrid features={features} />
          <ApacheBlock />
        </div>
      </div>
    );
  }
}

module.exports = Index;

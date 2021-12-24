import * as React from "react";
import Layout from "@theme/Layout";
import ReleaseTable from "../components/ReleaseTable";
import ConnectorTable from "../components/ConnectorTable";
import GuideTable from "../components/GuideTable";
import OldReleaseTable from "../components/OldReleaseTable";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";
import ReactMarkdown from "react-markdown";

const versions = require(`../../versions.json`);
const releases = require(`../../releases.json`);
const pulsarManagerReleases = require(`../../pulsar-manager-release.json`);
const pulsarAdaptersReleases = require(`../../pulsar-adapters-release.json`);
const connectors = require(`../../data/connectors.js`);

function getLatestArchiveMirrorUrl(version, type) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
}

function getLatestOffloadersMirrorUrl(version) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/apache-pulsar-offloaders-${version}-bin.tar.gz`;
}

function getLatestAdaptersMirrorUrl(version) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-adapters-${version}/apache-pulsar-adapters-${version}-src.tar.gz`;
}

function distUrl(version, type) {
  return `https://www.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
}

function distOffloadersUrl(version) {
  return `https://www.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-offloaders-${version}-bin.tar.gz`;
}

function distAdaptersUrl(version) {
  return `https://downloads.apache.org/pulsar/pulsar-adapters-${version}/apache-pulsar-adapters-${version}-src.tar.gz`;
}

function archiveUrl(version, type) {
  if (version.includes("incubating")) {
    return `https://archive.apache.org/dist/incubator/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
  } else {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
  }
}

function pularManagerArchiveUrl(version, type) {
  return `https://archive.apache.org/dist/pulsar/pulsar-manager/pulsar-manager-${version}/apache-pulsar-manager-${version}-${type}.tar.gz`;
}

function connectorDistUrl(name, version) {
  return `https://www.apache.org/dist/pulsar/pulsar-${version}/connectors/pulsar-io-${name}-${version}.nar`;
}

function connectorDownloadUrl(name, version) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/connectors/pulsar-io-${name}-${version}.nar`;
}

function getLatestPulsarManagerArchiveMirrorUrl(version, type) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-manager/pulsar-manager-${version}/apache-pulsar-manager-${version}-${type}.tar.gz`;
}

function pulsarManagerDistUrl(version, type) {
  return `https://www.apache.org/dist/pulsar/pulsar-manager/pulsar-manager-${version}/apache-pulsar-manager-${version}-${type}.tar.gz`;
}

function language(props) {
  return props.language ? props.language + "/" : "";
}

export default function page(props) {
  const { siteConfig } = useDocusaurusContext();
  const latestVersion = releases[0];
  const latestPulsarManagerVersion = pulsarManagerReleases[0];
  const latestPulsarAdaptersVersion = pulsarAdaptersReleases[0];
  const latestArchiveMirrorUrl = getLatestArchiveMirrorUrl(
    latestVersion,
    "bin"
  );
  const latestSrcArchiveMirrorUrl = getLatestArchiveMirrorUrl(
    latestVersion,
    "src"
  );
  const latestPulsarManagerArchiveMirrorUrl =
    getLatestPulsarManagerArchiveMirrorUrl(latestPulsarManagerVersion, "bin");
  const latestPulsarManagerSrcArchiveMirrorUrl =
    getLatestPulsarManagerArchiveMirrorUrl(latestPulsarManagerVersion, "src");
  const latestArchiveUrl = distUrl(latestVersion, "bin");
  const latestSrcArchiveUrl = distUrl(latestVersion, "src");
  const pulsarManagerLatestArchiveUrl = pulsarManagerDistUrl(
    latestPulsarManagerVersion,
    "bin"
  );
  const pulsarManagerLatestSrcArchiveUrl = pulsarManagerDistUrl(
    latestPulsarManagerVersion,
    "src"
  );

  const releaseInfo = releases.map((version) => {
    return {
      version: version,
      binArchiveUrl: archiveUrl(version, "bin"),
      srcArchiveUrl: archiveUrl(version, "src"),
    };
  });

  const pulsarManagerReleaseInfo = pulsarManagerReleases.map((version) => {
    return {
      version: version,
      binArchiveUrl: pularManagerArchiveUrl(version, "bin"),
      srcArchiveUrl: pularManagerArchiveUrl(version, "src"),
    };
  });

  const latest = [
    {
      release: "Binary",
      link: latestArchiveMirrorUrl,
      linkText: `apache-pulsar-${latestVersion}-bin.tar.gz`,
      asc: `${latestArchiveUrl}.asc`,
      sha512: `${latestArchiveUrl}.sha512`,
    },
    {
      release: "Source",
      link: latestSrcArchiveMirrorUrl,
      linkText: `apache-pulsar-${latestVersion}-src.tar.gz`,
      asc: `${latestSrcArchiveUrl}.asc`,
      sha512: `${latestSrcArchiveUrl}.sha512`,
    },
  ];
  const stable = [
    {
      release: "Source",
      link: getLatestAdaptersMirrorUrl(latestPulsarAdaptersVersion),
      linkText: `apache-pulsar-adapters-${latestPulsarAdaptersVersion}-src.tar.gz`,
      asc: `${distAdaptersUrl(latestPulsarAdaptersVersion)}.asc`,
      sha512: `${distAdaptersUrl(latestPulsarAdaptersVersion)}.sha512`,
    },
  ];
  const offloaders = [
    {
      release: "Offloaders",
      link: getLatestOffloadersMirrorUrl(latestVersion),
      linkText: `apache-pulsar-offloaders-${latestVersion}-bin.tar.gz`,
      asc: `${distOffloadersUrl(latestVersion)}.asc`,
      sha512: `${distOffloadersUrl(latestVersion)}.sha512`,
    },
  ];
  const guides = [
    {
      name: "The Pulsar java client",
      link: `${siteConfig.baseUrl}docs/${language(props)}client-libraries-java`,
      description: "The Pulsar java client",
    },
    {
      name: "The Pulsar go client",
      link: `${siteConfig.baseUrl}docs/${language(props)}client-libraries-go`,
      description: "The Pulsar go client",
    },
    {
      name: "The Pulsar python client",
      link: `${siteConfig.baseUrl}docs/${language(
        props
      )}client-libraries-python`,
      description: "The Pulsar java client",
    },
    {
      name: "The Pulsar C++ client",
      link: `${siteConfig.baseUrl}docs/${language(props)}client-libraries-cpp`,
      description: "The Pulsar C++ client",
    },
  ];
  const oldReleases = releaseInfo
    .filter((info) => {
      return info.version != latestVersion;
    })
    .map((info) => {
      var sha = "sha512";
      if (
        info.version.includes("1.19.0-incubating") ||
        info.version.includes("1.20.0-incubating")
      ) {
        sha = "sha";
      }
      return {
        release: info.version,
        binary: info.binArchiveUrl,
        binaryText: `apache-pulsar-${info.version}-bin.tar.gz`,
        binaryAsc: `${info.binArchiveUrl}.asc`,
        binarySha: `${info.binArchiveUrl}.${sha}`,
        binaryShaText: `${sha}`,
        source: info.srcArchiveUrl,
        sourceText: `apache-pulsar-${info.version}-src.tar.gz`,
        sourceAsc: `${info.srcArchiveUrl}.asc`,
        sourceSha: `${info.srcArchiveUrl}.${sha}`,
        sourceShaText: `${sha}`,
        releaseNote: `${siteConfig.baseUrl}${language(
          props
        )}release-notes#${info.version.replace(/\./g, "")}`,
      };
    });
  const apmStable = [
    {
      release: "Binary",
      link: latestPulsarManagerArchiveMirrorUrl,
      linkText: `apache-pulsar-manager-${latestPulsarManagerVersion}-bin.tar.gz`,
      asc: `${pulsarManagerLatestArchiveUrl}.asc`,
      sha512: `${pulsarManagerLatestArchiveUrl}.sha512`,
    },
    {
      release: "Source",
      link: latestPulsarManagerSrcArchiveMirrorUrl,
      linkText: `apache-pulsar-manager-${latestPulsarManagerVersion}-src.tar.gz`,
      asc: `${pulsarManagerLatestSrcArchiveUrl}.asc`,
      sha512: `${pulsarManagerLatestSrcArchiveUrl}.sha512`,
    },
  ];
  const apmOldReleases = pulsarManagerReleaseInfo
    .filter((info) => info.version !== latestPulsarManagerVersion)
    .map((info) => {
      const sha = "sha512";
      return {
        release: info.version,
        binary: info.binArchiveUrl,
        binaryText: `apache-pulsar-manager-${info.version}-bin.tar.gz`,
        binaryAsc: `${info.binArchiveUrl}.asc`,
        binarySha: `${info.binArchiveUrl}.${sha}`,
        binaryShaText: `${sha}`,
        source: info.srcArchiveUrl,
        sourceText: `apache-pulsar-manager-${info.version}-src.tar.gz`,
        sourceAsc: `${info.srcArchiveUrl}.asc`,
        sourceSha: `${info.srcArchiveUrl}.${sha}`,
        sourceShaText: `${sha}`,
        releaseNote: `${siteConfig.baseUrl}${language(props)}release-notes#${
          info.version
        }`,
      };
    });
  return (
    <Layout>
      <div className="tailwind">
        <div className="my-12 container">
          <header className="postHeader">
            <h1>
              <Translate>Apache Pulsar downloads</Translate>
            </h1>
            <hr />
          </header>

          <h2>
            <Translate>Release notes</Translate>
          </h2>
          <div>
            <p>
              <a href={`${siteConfig.baseUrl}${language(props)}release-notes`}>
                Release notes
              </a>{" "}
              for all Pulsar's versions
            </p>
          </div>
          <h2 id="latest">
            <Translate>Current version (Stable)</Translate> {latestVersion}
          </h2>
          <ReleaseTable data={latest}></ReleaseTable>
          <h3>
            <Translate>Tiered storage offloaders</Translate>
          </h3>
          <ReleaseTable data={offloaders}></ReleaseTable>
          <h3 id="connectors">
            <Translate>Pulsar IO connectors</Translate>
          </h3>
          <ConnectorTable
            data={connectors.map((connector) => {
              return {
                connector: connector.link,
                connectorText: connector.longName,
                archive: `${connectorDownloadUrl(
                  connector.name,
                  latestVersion
                )}`,
                archiveText: `pulsar-io-${connector.name}-${latestVersion}.nar`,
                asc: `${connectorDistUrl(connector.name, latestVersion)}.asc`,
                sha512: `${connectorDistUrl(
                  connector.name,
                  latestVersion
                )}.sha512`,
              };
            })}
          ></ConnectorTable>
          <h2>
            <Translate>Release Integrity</Translate>
          </h2>
          <ReactMarkdown>
            You must [verify](https://www.apache.org/info/verification.html) the
            integrity of the downloaded files. We provide OpenPGP signatures for
            every release file. This signature should be matched against the
            [KEYS](https://www.apache.org/dist/pulsar/KEYS) file which contains
            the OpenPGP keys of Pulsar's Release Managers. We also provide
            `SHA-512` checksums for every release file. After you download the
            file, you should calculate a checksum for your download, and make
            sure it is the same as ours.
          </ReactMarkdown>
          <h2>
            <Translate>Getting started</Translate>
          </h2>
          <div>
            <p>
              <Translate>
                Once you've downloaded a Pulsar release, instructions on getting
                up and running with a standalone cluster that you can run on
                your laptop can be found in the
              </Translate>
              &nbsp;
              <a
                href={`${siteConfig.baseUrl}docs/${language(props)}standalone`}
              >
                <Translate>Run Pulsar locally</Translate>
              </a>{" "}
              <Translate>tutorial</Translate>.
            </p>
          </div>
          <p>
            <Translate>
              If you need to connect to an existing Pulsar cluster or instance
              using an officially supported client, see the client docs for
              these languages:
            </Translate>
          </p>
          <GuideTable data={guides}></GuideTable>
          <h2 id="archive">
            <Translate>Older releases</Translate>
          </h2>
          <OldReleaseTable data={oldReleases}></OldReleaseTable>
          <header className="postHeader mt-12">
            <h1>
              <Translate>Pulsar Adapters</Translate>
            </h1>
            <hr />
          </header>
          <h2 id="latest">
            <Translate>Current version (Stable)</Translate>{" "}
            {latestPulsarAdaptersVersion}
          </h2>
          <ReleaseTable data={stable}></ReleaseTable>
          <Translate>
            Pulsar Adapters are available on Maven Central, there is no binary
            package.
          </Translate>
          <header className="postHeader mt-12">
            <h1>
              <Translate>Apache Pulsar Manager downloads</Translate>
            </h1>
            <hr />
          </header>
          <h2>
            <Translate>Release notes</Translate>
          </h2>
          <div>
            <p>
              <a
                href={`${siteConfig.baseUrl}${language(
                  props
                )}pulsar-manager-release-notes`}
              >
                Release notes
              </a>{" "}
              for all pulsar-manager's versions
            </p>
          </div>
          <h2 id="latest">
            <Translate>Current version (Stable)</Translate>{" "}
            {latestPulsarManagerVersion}
          </h2>
          <ReleaseTable data={apmStable}></ReleaseTable>
          <h2 id="pulsar-manager-archive">
            <Translate>Pulsar Manager older releases</Translate>
          </h2>
          <OldReleaseTable data={apmOldReleases}></OldReleaseTable>
        </div>
      </div>
    </Layout>
  );
}

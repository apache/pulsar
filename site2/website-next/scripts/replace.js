const replace = require("replace-in-file");

const fs = require("fs");

const CWD = process.cwd();
const siteConfig = require(`${CWD}/docusaurus.config.js`);
const nextDocsDir = `${CWD}/docs`;
const docsDir = `${CWD}/versioned_docs`;

function getVersions() {
  try {
    return JSON.parse(
      require("fs").readFileSync(`${CWD}/versions.json`, "utf8")
    );
  } catch (error) {
    //console.error(error)
    console.error("no versions found defaulting to 2.1.0");
  }
  return ["2.1.0"];
}

function downloadPageUrl() {
  return `${siteConfig.baseUrl}download`;
}

function binaryReleaseUrl(version) {
  if (version.includes("incubating")) {
    return `https://archive.apache.org/dist/incubator/pulsar/pulsar-${version}/apache-pulsar-${version}-bin.tar.gz`;
  } else {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-${version}-bin.tar.gz`;
  }
}

function connectorReleaseUrl(version) {
  if (version.includes("incubating")) {
    return `https://archive.apache.org/dist/incubator/pulsar/pulsar-${version}/apache-pulsar-io-connectors-${version}-bin.tar.gz`;
  } else if (version >= "2.3.0") {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/connectors`;
  } else {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-io-connectors-${version}-bin.tar.gz`;
  }
}

function offloaderReleaseUrl(version) {
  return `https://archive.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-offloaders-${version}-bin.tar.gz`;
}

function prestoPulsarReleaseUrl(version) {
  if (version.includes("incubating")) {
    return `https://archive.apache.org/dist/incubator/pulsar/pulsar-${version}/pulsar-presto-connector-${version}.tar.gz`;
  } else {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/pulsar-presto-connector-${version}.tar.gz`;
  }
}

function rpmReleaseUrl(version, type) {
  rpmVersion = version.replace("incubating", "1_incubating");
  if (version.includes("incubating")) {
    return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/pulsar/pulsar-${version}/RPMS/apache-pulsar-client${type}-${rpmVersion}.x86_64.rpm`;
  } else {
    return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/RPMS/apache-pulsar-client${type}-${rpmVersion}-1.x86_64.rpm`;
  }
}

function debReleaseUrl(version, type) {
  if (version.includes("incubating")) {
    return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/pulsar/pulsar-${version}/DEB/apache-pulsar-client${type}.deb`;
  } else {
    return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/DEB/apache-pulsar-client${type}.deb`;
  }
}

function rpmDistUrl(version, type) {
  rpmVersion = version.replace("incubating", "1_incubating");
  if (version.includes("incubating")) {
    return `https://archive.apache.org/dist/incubator/pulsar/pulsar-${version}/RPMS/apache-pulsar-client${type}-${rpmVersion}.x86_64.rpm`;
  } else {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/RPMS/apache-pulsar-client${type}-${rpmVersion}-1.x86_64.rpm`;
  }
}

function debDistUrl(version, type) {
  if (version.includes("incubating")) {
    return `https://archive.apache.org/dist/incubator/pulsar/pulsar-${version}/DEB/apache-pulsar-client${type}.deb`;
  } else {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/DEB/apache-pulsar-client${type}.deb`;
  }
}

function clientVersionUrl(version, type) {
  var versions = version.split(".");
  var majorVersion = parseInt(versions[0]);
  var minorVersion = parseInt(versions[1]);
  if (majorVersion === 2 && minorVersion < 5) {
    return `/api/` + type + `/` + version;
  } else if (majorVersion >= 2 && minorVersion >= 5) {
    return (
      `/api/` +
      type +
      `/` +
      majorVersion +
      `.` +
      minorVersion +
      `.0` +
      `-SNAPSHOT`
    );
  }
}

function doReplace(options) {
  replace(options)
    .then((changes) => {
      if (options.dry) {
        console.log("Modified files:");
        console.log(changes.join("\n"));
      }
    })
    .catch((error) => {
      console.error("Error occurred:", error);
    });
}

const versions = getVersions();

const latestVersion = versions[0];
const latestVersionWithoutIncubating = latestVersion.replace("-incubating", "");

const from = [
  /@pulsar:version_number@/g,
  /@pulsar:version@/g,
  /pulsar:binary_release_url/g,
  /pulsar:connector_release_url/g,
  /pulsar:offloader_release_url/g,
  /pulsar:presto_pulsar_connector_release_url/g,
  /pulsar:download_page_url/g,
  /@pulsar:rpm:client@/g,
  /@pulsar:rpm:client-debuginfo@/g,
  /@pulsar:rpm:client-devel@/g,
  /@pulsar:deb:client@/g,
  /@pulsar:deb:client-devel@/g,

  /@pulsar:dist_rpm:client@/g,
  /@pulsar:dist_rpm:client-debuginfo@/g,
  /@pulsar:dist_rpm:client-devel@/g,
  /@pulsar:dist_deb:client@/g,
  /@pulsar:dist_deb:client-devel@/g,

  /\/api\/python/g,
  /\/api\/cpp/g,
  /\/api\/pulsar-functions/g,
  /\/api\/client/g,
  /\/api\/admin/g,

  /@pulsar:version_number@/g,
];

const options = {
  files: [`${nextDocsDir}/*.md`, `${nextDocsDir}/**/*.md`],
  from: from,
  to: [
    `${latestVersionWithoutIncubating}`,
    `${latestVersion}`,
    binaryReleaseUrl(`${latestVersion}`),
    connectorReleaseUrl(`${latestVersion}`),
    offloaderReleaseUrl(`${latestVersion}`),
    prestoPulsarReleaseUrl(`${latestVersion}`),
    downloadPageUrl(),
    rpmReleaseUrl(`${latestVersion}`, ""),
    rpmReleaseUrl(`${latestVersion}`, "-debuginfo"),
    rpmReleaseUrl(`${latestVersion}`, "-devel"),
    debReleaseUrl(`${latestVersion}`, ""),
    debReleaseUrl(`${latestVersion}`, "-dev"),

    rpmDistUrl(`${latestVersion}`, ""),
    rpmDistUrl(`${latestVersion}`, "-debuginfo"),
    rpmDistUrl(`${latestVersion}`, "-devel"),
    debDistUrl(`${latestVersion}`, ""),
    debDistUrl(`${latestVersion}`, "-dev"),

    clientVersionUrl(`${latestVersion}`, "python"),
    clientVersionUrl(`${latestVersion}`, "cpp"),
    clientVersionUrl(`${latestVersion}`, "pulsar-functions"),
    clientVersionUrl(`${latestVersion}`, "client"),
    clientVersionUrl(`${latestVersion}`, "admin"),
    `${latestVersion}`,
  ],
  dry: false,
};

doReplace(options);

// TODO activate and test when first version of docs are cut
// replaces versions
for (v of versions) {
  // if (v === latestVersion) {
  //   continue;
  // }
  const vWithoutIncubating = v.replace("-incubating", "");
  const opts = {
    files: [
      `${docsDir}/version-${v}/*.md`,
      `${docsDir}/version-${v}/**/*.md`,
      // `${docsDir}/en/${v}/*.html`,
      // `${docsDir}/en/${v}/**/*.html`,
    ],
    from: from,
    to: [
      `${vWithoutIncubating}`,
      `${v}`,
      binaryReleaseUrl(`${v}`),
      connectorReleaseUrl(`${v}`),
      offloaderReleaseUrl(`${v}`),
      prestoPulsarReleaseUrl(`${v}`),
      downloadPageUrl(),
      rpmDistUrl(`${v}`, ""),
      rpmDistUrl(`${v}`, "-debuginfo"),
      rpmDistUrl(`${v}`, "-devel"),
      debDistUrl(`${v}`, ""),
      debDistUrl(`${v}`, "-dev"),
      rpmDistUrl(`${v}`, ""),
      rpmDistUrl(`${v}`, "-debuginfo"),
      rpmDistUrl(`${v}`, "-devel"),
      debDistUrl(`${v}`, ""),
      debDistUrl(`${v}`, "-dev"),
      clientVersionUrl(`${v}`, "python"),
      clientVersionUrl(`${v}`, "cpp"),
      clientVersionUrl(`${v}`, "pulsar-functions"),
      clientVersionUrl(`${v}`, "client"),
      clientVersionUrl(`${v}`, "admin"),
      `${v}`,
    ],
    dry: false,
  };
  doReplace(opts);
}

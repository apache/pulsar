This guide explains everything about Pulsar release notes.

# Intro to release notes

This chapter gives an overview of Pulsar release notes.

## Basic info

[Pulsar release notes](https://pulsar.apache.org/release-notes/) consist of the following parts.

Release note | Component
---|---
[Pulsar core](https://pulsar.apache.org/release-notes/#pulsar-release-notes)| Pulsar
[Pulsar clients](../../site2/docs/client-libraries.md) |- Java <br> - WebSocket <br> - C++ <br> - Python <br> - Go <br> - NodeJs <br> - C#

## Maintenance info

- The [Pulsar Release Note page](https://pulsar.apache.org/release-notes/) is generated automatically using [release-json-gen.sh](http://release-json-gen.sh).
  
  For implementation details, see [PIP 112: Generate Release Notes Automatically](https://github.com/apache/pulsar/wiki/PIP-112:-Generate-Release-Notes-Automatically).

- The [Pulsar Release Note page](https://pulsar.apache.org/release-notes/) info is fetched from the [Pulsar GitHub Releases Page](https://github.com/apache/pulsar/releases).
  
- The [Pulsar Release Note page](https://pulsar.apache.org/release-notes/) is updated when one of the following conditions is met:

  - A commit is pushed to the [pulsar-site repo](https://github.com/apache/pulsar-site). 
  
  - A [Pulsar site sync job](https://github.com/apache/pulsar-site/actions/workflows/ci-pulsar-website-docs-sync.yaml) is performed (every 6 hours).

# Submit release notes

Follow the steps below to submit release notes for Pulsar and clients.

1. On the [Pulsar Releases Page - GitHub](https://github.com/apache/pulsar/releases), add a release note for the new release.

    Component | Step 
    ---|---
    Pulsar core| Add a release note for it.
    Pulsar clients: <br> - Java <br> - WebSocket <br> - C++ <br> - Python | Add separate release notes for them.  <br> Example see the image below. <img title="a title" alt="Alt text" src="/../assets/release-note-guide-1.png">
    Pulsar clients  <br>- Go <br> - Node.js <br> - C#** | No action is needed. You do not need to take care of them since their release notes are synced from their repos to the Pulsar site .

    After the new release is published, all the information about the release is automatically added to the [Pulsar Release Note page](https://pulsar.apache.org/release-notes/).

    ![Java client release note example](../assets/release-note-guide-1.png)

2. After the website is updated and built successfully, check whether the release information is shown on the [Pulsar Release Note page](https://pulsar.apache.org/release-notes/). 






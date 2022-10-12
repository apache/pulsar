# Pulsar Release Note Guide

This guide explains everything about Pulsar release notes.

<!-- TOC -->
- [Intro to release notes](#intro-to-release-notes)
  - [Basic info](#basic-info)
  - [Maintenance info](#maintenance-info)
- [Submit release notes](#submit-release-notes)

<!-- /TOC -->

## Intro to release notes

This chapter gives an overview of Pulsar release notes.

### Basic info

[Pulsar release notes](https://pulsar.apache.org/release-notes/) consist of the following parts.

Release note | Component
---|---
[Pulsar core](https://pulsar.apache.org/release-notes/#pulsar-release-notes)| Pulsar
[Pulsar clients](../../site2/docs/client-libraries.md) |- Java <br><br> - WebSocket <br><br> - C++ <br><br> - Python <br><br> - Go <br><br> - NodeJs <br><br> - C#

### Maintenance info

For the [Pulsar Release Note page](https://pulsar.apache.org/release-notes/):

- It is generated automatically using [release-json-gen.sh](https://github.com/apache/pulsar-site/blob/main/site2/tools/release-json-gen.sh).
  
  For implementation details, see [PIP 112: Generate Release Notes Automatically](https://github.com/apache/pulsar/wiki/PIP-112:-Generate-Release-Notes-Automatically).

- The info is fetched from the [Pulsar Releases Page - GitHub](https://github.com/apache/pulsar/releases).
  
- It is updated when one of the following conditions is met:

  - A commit is pushed to the [pulsar-site repo](https://github.com/apache/pulsar-site). 
  
  - A [Pulsar site sync job](https://github.com/apache/pulsar-site/actions/workflows/ci-pulsar-website-docs-sync.yaml) is performed (every 6 hours).

## Submit release notes

Follow the steps below to submit release notes for Pulsar and clients.

1. On the [Pulsar Releases Page - GitHub](https://github.com/apache/pulsar/releases), add a release note for the new release.

    <table>
    <thead>
      <tr>
        <th colspan="2">Component</th>
        <th>Step</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td colspan="2">Pulsar core</td>
        <td>Add a release note for it.</td>
      </tr>
      <tr>
        <td rowspan="2">Pulsar clients</td>
        <td> - Java<br><br> - WebSocket<br><br> - C++<br><br> - Python</td>
        <td>Add separate release notes for them, that is, create independent sections in the release note.<br><br>Example<br><br><img title="Java client release note example" alt="Java client release note example" src="../assets/release-note-guide-1.png"></td>
      </tr>
      <tr>
        <td> - Go<br><br> - Node.js<br><br> - C#</td>
        <td>No action is needed. You do not need to take care of them since their release notes are synced from their repos to the <a href="https://pulsar.apache.org/release-notes/">Pulsar Release Note page</a>.</td>
      </tr>
    </tbody>
    </table>

    After the new release is published, all the information about the release is automatically added to the [Pulsar Release Note page](https://pulsar.apache.org/release-notes/).

2. Check whether the release information is shown on the [Pulsar Release Note page](https://pulsar.apache.org/release-notes/) after the website is updated and built successfully.
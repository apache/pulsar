

# The Pulsar website and documentation

This `README` is basically the meta-documentation for the Pulsar website and documentation. Here you'll find instructions on running the site locally

## Tools

Framework [Docusaurus](https://docusaurus.io/).

Ensure you have the latest version of [Node](https://nodejs.org/en/download/) installed. We also recommend you install [Yarn](https://yarnpkg.com/en/docs/install) as well.

> You have to be on Node >= 8.x and Yarn >= 1.5.


## Running the site locally

To run the site locally:

```bash
cd website
yarn install
yarn start
```

Note that the `/docs/en/` path shows the documentation for the latest stable release of Pulsar.  Change it to `/docs/en/next/` to show your local changes, with live refresh.

## Contribute

The website is comprised of two parts, one is documentation, while the other is website pages (including blog posts).

Documentation related pages are placed under `docs` directory. They are written in [Markdown](http://daringfireball.net/projects/markdown/syntax).
All documentation pages are versioned. See more details in [versioning](#versioning) section.

Website pages are non-versioned. They are placed under `website` directory.

### Documentation

#### Layout

All the markdown files are placed under `docs` directory. It is a flat structure.

```
├── docs
│   ├── adaptors-kafka.md
│   ├── adaptors-spark.md
│   ├── adaptors-storm.md
│   ├── admin-api-brokers.md
│   ├── admin-api-clusters.md
│   ├── admin-api-namespaces.md
│   ├── admin-api-non-persistent-topics.md
│   ├── admin-api-overview.md
│   ├── admin-api-partitioned-topics.md
│   ├── admin-api-permissions.md
│   ├── admin-api-persistent-topics.md
│   ├── admin-api-tenants.md
│   ├── administration-dashboard.md
│   ├── administration-geo.md
│   ├── administration-load-distribution.md
│   ├── administration-proxy.md
...
```

All the files are named in the following convention:

```
<category>-<page-name>.md
```

`<category>` is the category within the sidebard that this file belongs to, while `<page-name>` is the string to name the file within this category.

There isn't any constraints on how files are named. It is just a naming convention for better maintenance.

#### Document

##### Markdown Headers

All the documents are the usual Markdown files. However you need to add some Docusaurus-specific fields in Markdown headers in order to link them
correctly to the [Sidebar](#sidebar) and [Navigation Bar](#navigation).

`id`: A unique document id. If this field is not present, the document's id will default to its file name (without the extension).

`title`: The title of your document. If this field is not present, the document's title will default to its id.

`hide_title`: Whether to hide the title at the top of the doc.

`sidebar_label`: The text shown in the document sidebar for this document. If this field is not present, the document's `sidebar_label` will default to its title.

For example:

```bash
---
id: io-overview
title: Pulsar IO Overview
sidebar_label: Overview
---
```

##### Linking to another document

You can use relative URLs to other documentation files which will automatically get converted to the corresponding HTML links when they get rendered.

Example:

```md
[This links to another document](other-document.md)
```

This markdown will automatically get converted into a link to /docs/other-document.html (or the appropriately translated/versioned link) once it gets rendered.

This can help when you want to navigate through docs on GitHub since the links there will be functional links to other documents (still on GitHub),
but the documents will have the correct HTML links when they get rendered.

#### Linking to javadoc of pulsar class

We have a [remarkable plugin](https://github.com/jonschlinkert/remarkable) for generating links to the javadoc for pulsar classes.
You can write them in following syntax:

```shell
{@inject: javadoc:<Display Name>:<Relative-Path-To-Javadoc-Html-File>}
```

For example, following line generates a hyperlink to the javadoc of `PulsarAdmin` class.

```shell
{@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin.html}
```

#### Linking to files in Pulsar github repo

We are using same remarkable plugin to generate links to the files in Pulsar github repo.

You can write it using similar syntax:

```shell
{@inject: github:<Display Text>:<Relative-Path-To-Files>}
```

For example, following line generates a hyperlink to the dashboard Dockerfile.

```
{@inject: github:`Dockerfile`:/dashboard/Dockerfile}
```

For more details about markdown features, you can read [here](https://docusaurus.io/docs/en/doc-markdown).

#### Sidebar

All the sidebars are defined in a `sidebars.json` file under `website` directory. The documentation sidebar is named `docs` in that json structure.

When you want to add a page to sidebar, you can add the document `id` you used in the document header to existing sidebar/category. In the blow example,
`docs` is the name of the sidebar, "Getting started" is a category within the sidebar and "pulsar-2.0" is the `id` of one of the documents.

```bash
{
  "docs": {
    "Getting started": [
      "pulsar-2.0",
      "standalone",
      "standalone-docker",
      "client-libraries",
      "concepts-architecture"
    ],
    ...
  }
}
```

For more details about versioning, you can read [here](https://docusaurus.io/docs/en/navigation).

#### Navigation

To add links to the top navigation bar, you can add entries to the `headerLinks` of `siteConfig.js` under `website` directory.

See [Navigation and Sidebars](https://docusaurus.io/docs/en/navigation) in Docusaurus website to learn different types of links
you can add to the top navigation bar.

## Versioning

Documentation versioning with Docusaurus becomes simpler. When done with a new release, just simply run following command:

```shell
yarn run version ${version}
```

This will preserve all markdown files currently in `docs` directory and make them available as documentation for version `${version}`.
Versioned documents are placed into `website/versioned_docs/version-${version}`, where `${version}` is the version number
you supplied in the command above.

Versioned sidebars are also copied into `website/versioned_sidebars` and are named as `version-${version}-sidebars.json`.

If you wish to change the documentation for a past version, you can access the files for that respective version.

For more details about versioning, you can read [here](https://docusaurus.io/docs/en/versioning).

## Translation and Localization

Docusaurus allows for easy translation functionality using [Crowdin](https://crowdin.com/).
All the markdown files are written in English. These markdown files are uploaded to Crowdin
for translation by the users within a community. Top-level pages are also written in English.
The strings that are needed to be translated are wrapped in a `<translate>` tag.

[Pulsar Website Build](https://builds.apache.org/job/pulsar-website-build/) will automatically
pulling down and uploading translations for all the pulsar website documentation files. Once
it pulls down those translations from Crowdin, it will build those translations into the website.

### Contribute Translations

All the translations are stored and managed in this [Pulsar Crowdin project](https://crowdin.com/project/apache-pulsar).
If you would like to contribute translations, you can simply create a Crowdin account, join the project and make contributions.
Crowdin provides very good documentation for translators. You can read [those documentations](https://support.crowdin.com/crowdin-intro/)
to start contributing.

Your contributed translations will be licensed under [Apache License V2](https://www.apache.org/licenses/LICENSE-2.0).
Pulsar Committers will review those translations. If your translations are not reviewed or approved by any committers,
feel free to reach out to use via [slack channel](https://apache-pulsar.herokuapp.com/) or [mailing lists](https://pulsar.apache.org/contact/).

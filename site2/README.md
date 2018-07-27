

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
feel free to reach out to use via [slack channel](https://apache-pulsar.herokuapp.com/) or [mailing lists](https://pulsar.incubator.apache.org/contact/).

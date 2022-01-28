#!/usr/bin/env node

/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

require('@babel/register')({
    babelrc: false,
    only: [__dirname, `${process.cwd()}/core`],
    plugins: [
      require('./server/translate-plugin.js'),
      require('@babel/plugin-proposal-class-properties').default,
      require('@babel/plugin-proposal-object-rest-spread').default,
    ],
    presets: [
      require('@babel/preset-react').default,
      require('@babel/preset-env').default,
    ],
  });
  
  const program = require('commander');
  const chalk = require('chalk');
  const glob = require('glob');
  const fs = require('fs-extra');
  const mkdirp = require('mkdirp');
  const path = require('path');
  
  const readMetadata = require('./server/readMetadata.js');
  const utils = require('./server/utils.js');
  const versionFallback = require('./server/versionFallback.js');
  const metadataUtils = require('./server/metadataUtils.js');
  const env = require('./server/env.js');
  
  const CWD = process.cwd();
  let versions;
  if (fs.existsSync(`${CWD}/versions.json`)) {
    versions = require(`${CWD}/versions.json`);
  } else {
    versions = [];
  }
  
  let version;
  
  program
    .arguments('<version>')
    .action(ver => {
      version = ver;
    })
    .parse(process.argv);
  
  if (env.versioning.missingVersionsPage) {
    env.versioning.printMissingVersionsPageError();
    process.exit(1);
  }
  
  if (version.includes('/')) {
    console.error(
      `${chalk.red(
        'Invalid version number specified! Do not include slash (/). Try something like: 1.0.0',
      )}`,
    );
    process.exit(1);
  }
  
  if (typeof version === 'undefined') {
    console.error(
      `${chalk.yellow(
        'No version number specified!',
      )}\nPass the version you wish to create as an argument.\nEx: 1.0.0`,
    );
    process.exit(1);
  }
  
  if (versions.includes(version)) {
    console.error(
      `${chalk.yellow(
        'This version already exists!',
      )}\nSpecify a new version to create that does not already exist.`,
    );
    process.exit(1);
  }
  
  function makeHeader(metadata) {
    let header = '---\n';
    Object.keys(metadata).forEach(key => {
      header += `${key}: ${metadata[key]}\n`;
    });
    header += '---\n';
    return header;
  }
  
  function writeFileAndCreateFolder(file, content, encoding) {
    mkdirp.sync(path.dirname(file));
  
    fs.writeFileSync(file, content, encoding);
  }
  
  const versionFolder = `${CWD}/versioned_docs/version-${version}`;
  
  mkdirp.sync(versionFolder);
  
  // copy necessary files to new version, changing some of its metadata to reflect the versioning
  const files = glob.sync(`${CWD}/../${readMetadata.getDocsPath()}/**`);
  files.forEach(file => {
    const ext = path.extname(file);
    if (ext !== '.md' && ext !== '.markdown') {
      return;
    }
  
    const res = metadataUtils.extractMetadata(fs.readFileSync(file, 'utf8'));
    const metadata = res.metadata;
    // Don't version any docs without any metadata whatsoever.
    if (Object.keys(metadata).length === 0) {
      return;
    }
    const rawContent = res.rawContent;
    if (!metadata.id) {
      metadata.id = path.basename(file, path.extname(file));
    }
    if (metadata.id.includes('/')) {
      throw new Error('Document id cannot include "/".');
    }
    if (!metadata.title) {
      metadata.title = metadata.id;
    }
  
    const docsDir = path.join(CWD, '../', readMetadata.getDocsPath());
    const subDir = utils.getSubDir(file, docsDir);
    const docId = subDir ? `${subDir}/${metadata.id}` : metadata.id;
    // if (!versionFallback.diffLatestDoc(file, docId)) {
    //   return;
    // }
    metadata.original_id = metadata.id;
    metadata.id = `version-${version}-${metadata.id}`;
    const targetFile = subDir
      ? `${versionFolder}/${subDir}/${path.basename(file)}`
      : `${versionFolder}/${path.basename(file)}`;
  
    writeFileAndCreateFolder(
      targetFile,
      makeHeader(metadata) + rawContent,
      'utf8',
    );
  });
  
  // copy sidebar if necessary
  // if (versionFallback.diffLatestSidebar()) {
  mkdirp(`${CWD}/versioned_sidebars`);
  const sidebar = JSON.parse(fs.readFileSync(`${CWD}/sidebars.json`, 'utf8'));
  const versioned = {};

  Object.keys(sidebar).forEach(sb => {
    const versionSidebar = `version-${version}-${sb}`;
    versioned[versionSidebar] = {};

    const categories = sidebar[sb];
    Object.keys(categories).forEach(category => {
      versioned[versionSidebar][category] = [];

      const categoryItems = categories[category];
      categoryItems.forEach(categoryItem => {
        let versionedCategoryItem = categoryItem;
        if (typeof categoryItem === 'object') {
          if (categoryItem.ids && categoryItem.ids.length > 0) {
            versionedCategoryItem.ids = categoryItem.ids.map(
              id => `version-${version}-${id}`,
            );
          }
        } else if (typeof categoryItem === 'string') {
          versionedCategoryItem = `version-${version}-${categoryItem}`;
        }
        versioned[versionSidebar][category].push(versionedCategoryItem);
      });
    });
  });

  fs.writeFileSync(
    `${CWD}/versioned_sidebars/version-${version}-sidebars.json`,
    `${JSON.stringify(versioned, null, 2)}\n`,
    'utf8',
  );
  // }
  

  // update versions.json file
  versions.unshift(version);
  fs.writeFileSync(
    `${CWD}/versions.json`,
    `${JSON.stringify(versions, null, 2)}\n`,
  );
  
  console.log(`${chalk.green(`Version ${version} created!\n`)}`);
  
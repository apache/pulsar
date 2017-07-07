#!/bin/bash

BUNDLER_VERSION=$(cat package.json | jq .versions.bundler | tr -d '"')

for version in $(cat VERSIONS); do
  bundle _${BUNDLER_VERSION}_ exec jekyll build \
    --config _config.yml,docs/$version/_config.version.yml
done

git checkout asf-site

(
  cd $(git rev-parse --show-toplevel)
  rm -rf api css fonts img index.html ja js
  mv generated-site/* .
  git add -A
  git commit -m "New site build $(date)"
  git checkout master
  rm -rf api css fonts img index.html ja js
)

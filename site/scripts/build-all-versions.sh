#!/bin/bash

BUNDLER_VERSION=$(cat package.json | jq .versions.bundler | tr -d '"')

for version in $(cat VERSIONS); do
  bundle _${BUNDLER_VERSION}_ exec jekyll build \
    --config _config.yml,docs/$version/_config.version.yml
done

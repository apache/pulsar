#! /bin/sh
ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/site2/
mkdir -p .preview
cd .preview

if [ -d "$ROOT_DIR/site2/.preview/pulsar-site" ]; then
    git checkout .
    git pull origin main
else
    git clone -b main --depth 1 https://github.com/apache/pulsar-site.git
fi

cd $ROOT_DIR/site2/.preview/pulsar-site/site2/website-next
sh scripts/sync-docs.sh pulsar
sh preview.sh $@
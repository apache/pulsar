#! /bin/sh
ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/site2/
mkdir -p .preview
cd .preview

if [ -d "$ROOT_DIR/site2/.preview/pulsar-site" ]; then
    cd pulsar-site
    # git checkout .
    # git pull origin main
else
    git clone -b main --depth 1 https://github.com/apache/pulsar-site.git
fi

cd $ROOT_DIR/site2/.preview/pulsar-site/site2/website-next

sh scripts/sync-docs.sh $ROOT_DIR/site2

if [ "$?" = "0" ]; then
    echo "full sync done..."
else
    echo "Error: start fail, please update your local pulsar repo by run cmd: git pull" 1>&2
    exit 1
fi

node scripts/watch.js $ROOT_DIR/site2 $@

#! /bin/sh
ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/site2/
mkdir -p .preview
cd .preview

if [ -d "$ROOT_DIR/site2/.preview/pulsar-site" ]; then
    cd pulsar-site
    git clean -f
    git checkout .
    git pull origin main
else
    git clone -b main --depth 1 https://github.com/apache/pulsar-site.git
fi

cd $ROOT_DIR/site2/.preview/pulsar-site

if [ $1"" == "admin" ]; then
    sh site2/tools/pulsar-admin-md.sh $ROOT_DIR website
elif [ $1"" == "client" ]; then
    sh site2/tools/pulsar-client-md.sh $ROOT_DIR website
elif [ $1"" == "perf" ]; then
    sh site2/tools/pulsar-perf-md.sh $ROOT_DIR website
elif [ $1"" == "pulsar" ]; then
    sh site2/tools/pulsar-md.sh $ROOT_DIR website
else
    sh site2/tools/pulsar-perf-md.sh $ROOT_DIR website
    sh site2/tools/pulsar-client-md.sh $ROOT_DIR website
    sh site2/tools/pulsar-admin-md.sh $ROOT_DIR website
    sh site2/tools/pulsar-md.sh $ROOT_DIR website
fi

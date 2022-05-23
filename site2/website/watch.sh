#! /bin/sh
ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/site2/.preview/pulsar-site/site2/website-next

echo "monitoring file changes..."
while true; do
    sh scripts/sync-docs.sh pulsar
    sleep 1
done

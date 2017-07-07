#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
PROTO_FILE=pulsar-common/src/main/proto/PulsarApi.proto

export PATH=site/scripts/protoc-gen-doc:$PATH

(
  cd $(git rev-parse --show-toplevel)
  protoc --doc_out=json,protobuf.json:site/_data/ $PROTO_FILE
)

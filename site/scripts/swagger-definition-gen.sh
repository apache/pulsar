#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
SWAGGER_FILE=pulsar-broker/target/docs/swagger.json
DESTINATION=site/_data/admin-rest-api-swagger.json

(
  cd $ROOT_DIR

  mvn package -DskipTests
  cp $SWAGGER_FILE $DESTINATION
)

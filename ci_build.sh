#!/usr/bin/env bash

SCALA_VERSION=$1

if [ -z "$SCALA_VERSION" ]; then
  echo "Missing value for SCALA_VERSION"
  exit 1
fi

CURRENT=0
MAX_TRIES=3
COMMAND_STATUS=1
until [[ $COMMAND_STATUS -eq 0 || $CURRENT -eq ${MAX_TRIES} ]]; do
  sbt ++${SCALA_VERSION} ";akka-persistence-mongo-common/ci:test;akka-persistence-mongo-rxmongo/ci:test;akka-persistence-mongo-scala/ci:test;akka-persistence-mongo-tools/ci:test"
  COMMAND_STATUS=$?
  sleep 5
  let CURRENT=CURRENT+1
done
exit $COMMAND_STATUS
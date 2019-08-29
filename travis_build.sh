#!/usr/bin/env bash
if [[ $TRAVIS_SCALA_VERSION == 2.13* ]]; then
  sbt ++$TRAVIS_SCALA_VERSION ";akka-persistence-mongo-common/travis:test;akka-persistence-mongo-rxmongo/travis:test;akka-persistence-mongo-scala/travis:test;akka-persistence-mongo-tools/travis:test"
else
  sbt ++$TRAVIS_SCALA_VERSION ";akka-persistence-mongo-common/travis:test;akka-persistence-mongo-casbah/travis:test;akka-persistence-mongo-rxmongo/travis:test;akka-persistence-mongo-scala/travis:test;akka-persistence-mongo-tools/travis:test"
fi

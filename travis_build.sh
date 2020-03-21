#!/usr/bin/env bash

sbt ++$TRAVIS_SCALA_VERSION ";akka-persistence-mongo-common/travis:test;akka-persistence-mongo-rxmongo/travis:test;akka-persistence-mongo-scala/travis:test;akka-persistence-mongo-tools/travis:test"

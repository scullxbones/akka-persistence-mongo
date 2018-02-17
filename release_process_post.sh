#!/usr/bin/env bash

git add .
git commit -m 'Prepare for '$NEXT' release'
git tag -a $NEXT -m '' -s

sbt +publishSigned
sbt sonatypeReleaseAll

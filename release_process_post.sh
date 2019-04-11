#!/usr/bin/env bash

git push origin master
git push origin --tags

sbt +publishSigned
sbt sonatypeReleaseAll

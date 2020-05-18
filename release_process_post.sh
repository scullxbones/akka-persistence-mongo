#!/usr/bin/env bash

git push origin akka25
git push origin --tags

sbt "; +publishSigned; sonatypeBundleRelease"

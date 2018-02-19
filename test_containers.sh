#!/bin/bash

MONGODB_VERSION=3.6
MONGODB_NOAUTH_PORT=27117
MONGODB_AUTH_PORT=28117
MONGODB_OPTS="--storageEngine wiredTiger --bind_ip_all"

docker pull scullxbones/mongodb:$MONGODB_VERSION
docker ps -a | grep scullxbones/mongodb | awk '{print $1}' | xargs docker rm -f
sleep 3

docker run -d -p $MONGODB_NOAUTH_PORT:27017 scullxbones/mongodb:$MONGODB_VERSION --noauth $MONGODB_OPTS
docker run -d -p $MONGODB_AUTH_PORT:27017 scullxbones/mongodb:$MONGODB_VERSION --auth $MONGODB_OPTS

sleep 3
docker exec $(docker ps -a | grep -e "--auth" | awk '{print $1;}') mongo admin --eval "db.createUser({user:'admin',pwd:'password',roles:['root']});"

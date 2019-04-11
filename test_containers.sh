#!/bin/bash

MONGODB_VERSION=3.6
MONGODB_NOAUTH_PORT=27117
MONGODB_AUTH_PORT=28117
MONGODB_OPTS="--storageEngine wiredTiger --bind_ip_all"

docker pull scullxbones/mongodb:$MONGODB_VERSION
docker ps -a | grep scullxbones/mongodb | awk '{print $1}' | xargs docker rm -f
sleep 3

docker run --rm --name mongo_noauth -d -p $MONGODB_NOAUTH_PORT:27017 scullxbones/mongodb:$MONGODB_VERSION --noauth $MONGODB_OPTS
docker run --rm --name mongo_auth -d -p $MONGODB_AUTH_PORT:27017 scullxbones/mongodb:$MONGODB_VERSION --auth $MONGODB_OPTS

sleep 3
docker exec mongo_auth mongo admin --eval "db.createUser({user:'admin',pwd:'password',roles:['root']});"

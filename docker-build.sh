#!/bin/bash

NAME=spark-lineage
VERSION=1.0.0

docker build -t $NAME:$VERSION .
docker tag $NAME:latest your-username/$NAME:$VERSION
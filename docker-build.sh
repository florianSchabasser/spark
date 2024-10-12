#!/bin/bash

DOCKER_USER=floriansdocker
IMAGE_NAME=spark-lineage
IMAGE_VERSION=3.5.3.0

docker build -t $IMAGE_NAME:$IMAGE_VERSION .
docker tag $IMAGE_NAME:$IMAGE_VERSION $DOCKER_USER/$IMAGE_NAME:$IMAGE_VERSION
docker push $DOCKER_USER/$IMAGE_NAME:$IMAGE_VERSION
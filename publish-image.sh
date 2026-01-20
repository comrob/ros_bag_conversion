#!/bin/bash
set -e
ORG="comrob"
IMAGE="ros_bag_converter"
TAG="latest"
FULL="ghcr.io/$ORG/$IMAGE:$TAG"

echo "Building and Pushing: $FULL"
docker build -t "$FULL" .
docker push "$FULL"
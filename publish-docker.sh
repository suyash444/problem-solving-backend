#!/bin/bash
set -e

IMAGE_NAME=costarcc/problem-solving-api
VERSION=V1.00

echo "Building image ${IMAGE_NAME}:${VERSION}"
docker build -t ${IMAGE_NAME}:${VERSION} .

echo "Pushing image ${IMAGE_NAME}:${VERSION}"
docker push ${IMAGE_NAME}:${VERSION}

echo "Done"

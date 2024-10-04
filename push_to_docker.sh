#!/bin/bash

# Set your Docker Hub username
DOCKERHUB_USERNAME="tytheprefectionist"

# Get the git commit hash
GIT_HASH=$(git rev-parse --short HEAD)

# Build the Docker image
docker build -t prefect-demo .

# Tag the image with the git hash
docker tag prefect-demo $DOCKERHUB_USERNAME/prefect-demo:$GIT_HASH

# Log in to Docker Hub
echo "Please enter your Docker Hub password:"
docker login 

# Push the image to Docker Hub
docker push $DOCKERHUB_USERNAME/prefect-demo:$GIT_HASH

echo "Image pushed successfully to Docker Hub with tag: $GIT_HASH"


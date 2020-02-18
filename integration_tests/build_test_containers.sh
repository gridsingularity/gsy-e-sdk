#!/usr/bin/env bash

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    docker_command="sudo docker"
else
    docker_command="docker"
fi;

if [[ "$(${docker_command} images -q d3a-api-client 2> /dev/null)" == "" ]]; then
  echo "Building d3a-api-client image ..."
  ${docker_command} build -t d3a-api-client .
  echo "... done"
fi

D3A_IMAGE_TAG="d3a-tests"

if [[ "$(${docker_command} images -q ${D3A_IMAGE_TAG} 2> /dev/null)" == "" ]]; then
  echo "Building d3a image ..." && \
  rm -rf tests/d3a && \
  cd tests/ && \
  git clone -b feature/D3ASIM-1979 https://github.com/gridsingularity/d3a.git && \
  cd d3a && \
  ${docker_command} build -t ${D3A_IMAGE_TAG} . && \
  cd ../ && \
  rm -rf d3a/ && \
  cd ../ && \
  echo ".. done"
fi

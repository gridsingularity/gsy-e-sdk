#!/usr/bin/env bash

set -e

D3A_IMAGE_TAG="d3a-tests"

if [[ "$(docker images -q ${D3A_IMAGE_TAG} 2> /dev/null)" == "" ]]; then
  echo "Building d3a image ..." && \
  rm -rf tests/d3a && \
  cd tests/ && \
  git clone https://github.com/gridsingularity/d3a.git && \
  cd d3a && \
  docker build -t ${D3A_IMAGE_TAG} . && \
  cd ../ && \
  rm -rf d3a/ && \
  cd ../ && \
  echo ".. done"
fi

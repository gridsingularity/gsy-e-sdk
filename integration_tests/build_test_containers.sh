#!/usr/bin/env bash

set -e

GSY_E_IMAGE_TAG="gsy-e-tests"

if [[ "$(docker images -q ${GSY_E_IMAGE_TAG} 2> /dev/null)" == "" ]]; then
  echo "Building d3a image ..." && \
  rm -rf tests/d3a && \
  cd tests/ && \
  git clone https://github.com/gridsingularity/gsy-e.git && \
  cd gsy-e && \
  docker build -t ${GSY_E_IMAGE_TAG} . && \
  cd ../ && \
  rm -rf gsy-e/ && \
  cd ../ && \
  echo ".. done"
fi

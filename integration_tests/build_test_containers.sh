#!/usr/bin/env bash

if [[ "$(docker images -q d3a-api-client 2> /dev/null)" == "" ]]; then
  docker build -t d3a-api-client .
fi

if [[ "$(docker images -q d3a 2> /dev/null)" == "" ]]; then
  cd tests/ && \
  git clone https://github.com/gridsingularity/d3a.git && \
  cd d3a && \
  docker build -t d3a . && \
  cd ../ && \
  rm -rf d3a/ && \
  cd ../
fi

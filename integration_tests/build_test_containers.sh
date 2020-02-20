#!/usr/bin/env bash

if [[ "$(docker images -q d3a-tests 2> /dev/null)" == "" ]]; then
  cd tests/ && \
  cp -rf ~/dev/d3a . && \
#  git clone https://github.com/gridsingularity/d3a.git && \
  cd d3a && \
  docker build -t d3a-tests . && \
  cd ../ && \
  rm -rf d3a/ && \
  cd ../
fi

#! /bin/bash

docker run \
    --rm -it \
    -p 50070:50070 \
    -p 8088:8088 \
    -p 8888:8888 \
    -p 9000:9000 \
    -v "$PWD":/app \
    jdvelasq/hadoop:2.8.5  sh
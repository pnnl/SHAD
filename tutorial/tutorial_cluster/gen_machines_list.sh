#!/bin/bash

docker ps --format '{{ .Names }}\t{{ .ID }}' | awk '/node/ {print $2}' > machines

HEAD_NODE=$(docker ps --format '{{ .Names }}' | awk '/head/')

docker cp machines $HEAD_NODE:/home/tutorial/machines

rm machines

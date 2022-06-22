#!/bin/bash

docker ps --format '{{ .Names }}\t{{ .ID }}' | awk '/shad/ && /node/ {print $2}' > machines

HEAD_NODE=$(docker ps --format '{{ .Names }}' | awk '/shad/ && /head/')

docker cp machines $HEAD_NODE:/home/tutorial/machines

rm machines

#!/bin/bash

master=$(curl -s -X GET http://127.0.0.1:10086/master)
curl -s -X POST http://${master}/project?proj=Lianjia-Crawler
for i in $(seq 90); do
    echo "======================================="
    curl -s -X GET http://${master}/project/status
    sleep 1
done

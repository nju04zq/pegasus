#!/bin/bash

master=$(curl -s -X GET http://127.0.0.1:10086/master)
curl -s -X GET http://${master}/test

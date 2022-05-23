#!/bin/bash
docker network create mongonet
docker run -i -t -p 27017:27017 --name mongo_server --network=mongonet mongo:latest /usr/bin/mongod --bind_ip_all
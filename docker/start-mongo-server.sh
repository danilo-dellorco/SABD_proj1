#!/bin/bash
docker run -i -t -p 27017:27017 --name mongo-server --network=proj1_network mongo:latest /usr/bin/mongod --bind_ip_all
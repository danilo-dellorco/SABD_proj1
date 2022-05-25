#!/bin/bash
docker run -i -t -p 27017:27017 --name mongo-server mongo:latest /usr/bin/mongod --bind_ip_all
